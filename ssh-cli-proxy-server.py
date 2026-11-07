#!/usr/bin/env python3
"""Keep an ssh connection to a host, and run commands there for clients.

Usage: ssh-cli-proxy-server.py DESTINATION PORT

DESTINATION is that of ssh, such as user@host, and PORT is the port on
localhost the clients connect to, with the token in
~/.ssh-cli-proxy/PORT.token, readable by the user only.  Commands run
by ssh-cli-proxy-client.py cost a round trip only, as through the
connection, instead of connecting each time.

The commands are run by an agent run by python3 on the host, with
their args, not by a shell.  Their stdin, stdout and stderr are
streamed by frames, through the connection multiplexed by the id of
the streams:

  type (1 byte), stream id (4 bytes), length (4 bytes), payload

  OPEN    client -> agent  JSON of the args, and the token from clients
  STDIN   client -> agent  the data
  EOF     client -> agent  the end of stdin
  KILL    client -> agent  the signal number, sent if the client is gone
  STDOUT  agent -> client  the data
  STDERR  agent -> client  the data
  EXIT    agent -> client  the exit code, as of a shell if signaled

A client has a TCP connection of its own, with the stream id 0.  The
connection to the host is started again by the next command if
exited, failing the commands running with the exit code 255.

The commands and the connection are logged to stderr and
~/.ssh-cli-proxy/PORT.log, without the data.
"""

import argparse
import asyncio
import json
import logging
import secrets
import shlex
import signal
import struct
import sys
import time
from pathlib import Path

STATE_DIRECTORY = Path.home() / ".ssh-cli-proxy"

# Compressed, as the output, such as of a terminal, is large but repetitive.
SSH_OPTIONS = ["-o", "BatchMode=yes", "-o", "ServerAliveInterval=30",
               "-o", "Compression=yes"]

OPEN, STDIN, EOF, KILL, STDOUT, STDERR, EXIT = range(1, 8)
HEADER = struct.Struct("!BII")
SIGKILL = 9

# Run by python3 on the host, with the frames on its stdin and stdout.
AGENT = r'''
import asyncio, json, os, struct
OPEN, STDIN, EOF, KILL, STDOUT, STDERR, EXIT = range(1, 8)
HEADER = struct.Struct("!BII")
# The stdin of each stream: its data, None for EOF, or an int to kill.
queues = {}

def send(writer, kind, stream_id, payload=b""):
    writer.write(HEADER.pack(kind, stream_id, len(payload)) + payload)

async def pump(writer, stream, kind, stream_id):
    while data := await stream.read(65536):
        send(writer, kind, stream_id, data)
        await writer.drain()

async def feed(process, queue):
    # In order, also what comes before the process is started.
    while True:
        item = await queue.get()
        try:
            if isinstance(item, int):
                process.send_signal(item)
            elif item is None:
                process.stdin.close()
            else:
                process.stdin.write(item)
                await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError, ProcessLookupError):
            pass

async def run(writer, stream_id, args, queue):
    try:
        process = await asyncio.create_subprocess_exec(
            *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
    except Exception as error:
        send(writer, STDERR, stream_id, f"{error}\n".encode())
        code = 127
    else:
        feeding = asyncio.create_task(feed(process, queue))
        await asyncio.gather(pump(writer, process.stdout, STDOUT, stream_id),
                             pump(writer, process.stderr, STDERR, stream_id))
        code = await process.wait()
        feeding.cancel()
    del queues[stream_id]
    send(writer, EXIT, stream_id, struct.pack("!i", code if code >= 0 else 128 - code))
    await writer.drain()

async def main():
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=2 ** 24)
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader),
                                 os.fdopen(0, "rb", 0))
    transport, protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, os.fdopen(1, "wb", 0))
    writer = asyncio.StreamWriter(transport, protocol, None, loop)
    tasks = set()
    while True:
        try:
            kind, stream_id, length = HEADER.unpack(await reader.readexactly(HEADER.size))
            payload = await reader.readexactly(length)
        except asyncio.IncompleteReadError:
            break
        if kind == OPEN:
            queues[stream_id] = asyncio.Queue()
            task = asyncio.create_task(run(writer, stream_id, json.loads(payload),
                                           queues[stream_id]))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        elif stream_id in queues:
            queues[stream_id].put_nowait(payload if kind == STDIN else
                                         None if kind == EOF else
                                         struct.unpack("!i", payload)[0])
    # Disconnected, so kill all by SIGKILL.
    for queue in queues.values():
        queue.put_nowait(9)
    await asyncio.sleep(0.1)

asyncio.run(main())
'''


async def read_frame(reader):
    """Return (type, stream id, payload) of the next frame, or None at EOF."""
    try:
        kind, stream_id, length = HEADER.unpack(await reader.readexactly(HEADER.size))
        return kind, stream_id, await reader.readexactly(length)
    except (asyncio.IncompleteReadError, ConnectionError):
        return None


def write_frame(writer, kind, stream_id, payload=b""):
    writer.write(HEADER.pack(kind, stream_id, len(payload)) + payload)


class Stream:
    """A command of a client, through the connection."""

    def __init__(self, stream_id, writer):
        self.id = stream_id
        self.writer = writer
        self.start = time.monotonic()
        self.sizes = {STDIN: 0, STDOUT: 0, STDERR: 0}
        self.done = asyncio.Event()

    def exit(self, code):
        self.done.set()
        log = logging.info if code == 0 else logging.warning
        log("#%d exited with %d in %.2fs, "
            "stdin %d bytes, stdout %d bytes, stderr %d bytes",
            self.id, code, time.monotonic() - self.start,
            self.sizes[STDIN], self.sizes[STDOUT], self.sizes[STDERR])


class Connection:
    """The ssh connection to the agent, with the streams through it."""

    def __init__(self, destination):
        self.destination = destination
        self.process = None
        self.streams = {}
        self.last_id = 0
        self.lock = asyncio.Lock()

    async def ensure(self):
        """Start the connection unless running."""
        async with self.lock:
            if self.process and self.process.returncode is None:
                return
            self.process = await asyncio.create_subprocess_exec(
                "ssh", *SSH_OPTIONS, self.destination,
                "python3 -u -c " + shlex.quote(AGENT),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, limit=2 ** 24)
            self.streams = {}
            logging.info("ssh %s started, pid %d", self.destination, self.process.pid)
            asyncio.create_task(self.read(self.process, self.streams))
            asyncio.create_task(self.read_stderr(self.process))

    def stop(self):
        if self.process and self.process.returncode is None:
            self.process.kill()

    async def read(self, process, streams):
        """Pass the frames from PROCESS to the clients of STREAMS."""
        while frame := await read_frame(process.stdout):
            kind, stream_id, payload = frame
            stream = streams.get(stream_id)
            if stream is None:
                continue
            if kind in (STDOUT, STDERR):
                stream.sizes[kind] += len(payload)
            write_frame(stream.writer, kind, 0, payload)
            if kind == EXIT:
                del streams[stream_id]
                stream.exit(struct.unpack("!i", payload)[0])
            try:
                await stream.writer.drain()
            except ConnectionError:
                pass
        # Exited, so are the streams through it.
        logging.warning("ssh %s exited with %s, failing %d commands",
                        self.destination, await process.wait(), len(streams))
        for stream in list(streams.values()):
            write_frame(stream.writer, STDERR, 0, b"ssh-cli-proxy: connection lost\n")
            write_frame(stream.writer, EXIT, 0, struct.pack("!i", 255))
            stream.exit(255)
        streams.clear()

    async def read_stderr(self, process):
        while line := await process.stderr.readline():
            logging.warning("ssh: %s", line.decode(errors="replace").rstrip())

    async def send(self, kind, stream_id, payload=b""):
        write_frame(self.process.stdin, kind, stream_id, payload)
        await self.process.stdin.drain()

    async def serve(self, args, reader, writer):
        """Run ARGS for a client by READER and WRITER, until exited."""
        await self.ensure()
        self.last_id += 1
        stream = Stream(self.last_id, writer)
        self.streams[stream.id] = stream
        logging.info("#%d %s", stream.id, shlex.join(args))
        try:
            await self.send(OPEN, stream.id, json.dumps(args).encode())
            while not stream.done.is_set() and (frame := await read_frame(reader)):
                kind, _, payload = frame
                if kind == STDIN:
                    stream.sizes[STDIN] += len(payload)
                if kind in (STDIN, EOF):
                    await self.send(kind, stream.id, payload)
                if kind == EOF:
                    break
            # Wait for the exit, or kill if the client is gone.
            done = asyncio.create_task(stream.done.wait())
            gone = asyncio.create_task(reader.read())
            await asyncio.wait({done, gone}, return_when=asyncio.FIRST_COMPLETED)
            gone.cancel()
            if not stream.done.is_set():
                logging.warning("#%d killed, as the client is gone", stream.id)
                await self.send(KILL, stream.id, struct.pack("!i", SIGKILL))
                await stream.done.wait()
        except ConnectionError:
            pass


def make_handler(connection, token):
    """Return the function handling the connection of a client."""

    async def handle(reader, writer):
        try:
            frame = await read_frame(reader)
            if frame is None or frame[0] != OPEN:
                return
            request = json.loads(frame[2])
            if not secrets.compare_digest(request.get("token", ""), token):
                logging.warning("rejected a client without the token")
                return
            await connection.serve(request["args"], reader, writer)
            await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    return handle


async def serve(destination, port, token_file):
    """Serve the clients on PORT until interrupted, or return False if failed."""
    connection = Connection(destination)
    token = secrets.token_hex(16)
    try:
        server = await asyncio.start_server(make_handler(connection, token),
                                            "127.0.0.1", port)
    except OSError as error:
        logging.error("cannot listen on port %d: %s", port, error)
        return False
    # Only after listening, not to replace the token of another server.
    token_file.touch(mode=0o600)
    token_file.write_text(token)
    logging.info("ssh-cli-proxy for %s on port %d", destination, port)
    try:
        await connection.ensure()
        async with server:
            await server.serve_forever()
    finally:
        token_file.unlink(missing_ok=True)
        connection.stop()
        logging.info("ssh-cli-proxy for %s on port %d stopped", destination, port)


def main():
    parser = argparse.ArgumentParser(description="Keep an ssh connection to a host, "
                                     "and run commands there for clients.")
    parser.add_argument("destination", help="the destination of ssh, such as user@host")
    parser.add_argument("port", type=int, help="the port on localhost for the clients")
    arguments = parser.parse_args()

    STATE_DIRECTORY.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(STATE_DIRECTORY / f"{arguments.port}.log",
                                      encoding="utf-8")])
    # Even if ignored as inherited, and by kill also.
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        if asyncio.run(serve(arguments.destination, arguments.port,
                             STATE_DIRECTORY / f"{arguments.port}.token")) is False:
            sys.exit(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
