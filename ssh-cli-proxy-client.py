#!/usr/bin/env python3
"""Run a command on a host by ssh-cli-proxy-server.py, as ssh does.

Usage: ssh-cli-proxy-client.py PORT COMMAND [ARG]...

PORT is that of the server on localhost, with the token in
~/.ssh-cli-proxy/PORT.token.  The stdin is streamed to the command,
and its stdout and stderr back, until it exits with its exit code.
Interrupting kills the command.  The exit code is 255 if failed by
the proxy, as of ssh.
"""

import json
import os
import socket
import struct
import sys
import threading
from pathlib import Path

STATE_DIRECTORY = Path.home() / ".ssh-cli-proxy"

OPEN, STDIN, EOF, KILL, STDOUT, STDERR, EXIT = range(1, 8)
HEADER = struct.Struct("!BII")


def fail(message):
    print(f"ssh-cli-proxy: {message}", file=sys.stderr, flush=True)
    # Not waiting for the thread reading the stdin.
    os._exit(255)


class Channel:
    """The connection to the server, by frames of the stream 0."""

    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port))
        # Not blocking forever, to be interrupted on Windows also.
        self.socket.settimeout(0.5)
        self.lock = threading.Lock()

    def send(self, kind, payload=b""):
        with self.lock:
            self.socket.sendall(HEADER.pack(kind, 0, len(payload)) + payload)

    def receive_exactly(self, size):
        data = bytearray()
        while len(data) < size:
            try:
                chunk = self.socket.recv(size - len(data))
            except socket.timeout:
                continue
            if not chunk:
                return None
            data += chunk
        return bytes(data)

    def receive(self):
        """Return (type, payload) of the next frame, or None at EOF."""
        header = self.receive_exactly(HEADER.size)
        if header is None:
            return None
        kind, _, length = HEADER.unpack(header)
        payload = self.receive_exactly(length)
        return None if payload is None else (kind, payload)


def send_stdin(channel):
    """Stream the stdin, until EOF."""
    try:
        if sys.stdin is not None:
            while data := sys.stdin.buffer.read1(65536):
                channel.send(STDIN, data)
        channel.send(EOF)
    except OSError:
        pass


def main():
    if len(sys.argv) < 3 or not sys.argv[1].isdigit():
        fail("usage: ssh-cli-proxy-client.py PORT COMMAND [ARG]...")
    port, args = int(sys.argv[1]), sys.argv[2:]
    try:
        token = (STATE_DIRECTORY / f"{port}.token").read_text().strip()
        channel = Channel(port)
    except (FileNotFoundError, ConnectionRefusedError):
        fail(f"server on port {port} is not running")
    channel.send(OPEN, json.dumps({"token": token, "args": args}).encode())
    threading.Thread(target=send_stdin, args=(channel,), daemon=True).start()
    outputs = {STDOUT: sys.stdout.buffer, STDERR: sys.stderr.buffer}
    try:
        while frame := channel.receive():
            kind, payload = frame
            if kind in outputs:
                outputs[kind].write(payload)
                outputs[kind].flush()
            elif kind == EXIT:
                # Not waiting for the thread reading the stdin.
                os._exit(struct.unpack("!i", payload)[0])
    except KeyboardInterrupt:
        # The server kills the command, as the client is gone.
        fail("interrupted")
    fail("connection to the server lost")


if __name__ == "__main__":
    main()
