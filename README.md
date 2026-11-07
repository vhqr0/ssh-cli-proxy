# ssh-cli-proxy

Run commands on a remote host through one long-lived ssh connection,
so each command costs a round trip instead of a new ssh handshake.

On a slow link, connecting by ssh takes a dozen round trips, several
seconds each time.  `ssh-cli-proxy-server.py` keeps one connection
open, and `ssh-cli-proxy-client.py` runs a command through it, like
`ssh HOST COMMAND...`, streaming the stdin, stdout and stderr, and
passing back the exit code.

Only the Python standard library is needed: Python 3.8 or later
locally, and `python3` on the host.  Your ssh config and keys are
used as they are, by the `ssh` command.

## Usage

Start the server, keeping it running:

```sh
ssh-cli-proxy-server.py user@host 2222
```

The first argument is the destination of ssh, such as `user@host` or a
host of `~/.ssh/config`.  The second is the port on localhost the
clients connect to.  The connection must not ask for a password, as
ssh runs with `BatchMode=yes`.

Run commands through it:

```sh
ssh-cli-proxy-client.py 2222 tmux list-panes -a
echo hello | ssh-cli-proxy-client.py 2222 cat
```

The command runs with its args as they are, not by a shell, in the
home directory.  To change the directory or the environment, use
`env`, or a shell explicitly:

```sh
ssh-cli-proxy-client.py 2222 env -C /path FOO=bar git status
ssh-cli-proxy-client.py 2222 sh -c 'cd /path && make'
```

The exit code is that of the command, or 255 if failed by the proxy,
as of ssh.  Interrupting or killing the client kills the command.

### Wrapping a command

A script can make a remote command look local, such as `tmux` on
Windows, as `tmux.cmd` beside the client:

```bat
@python "%~dp0ssh-cli-proxy-client.py" 2222 tmux %*
```

or as `tmux` on a POSIX system:

```sh
#!/bin/sh
exec ssh-cli-proxy-client.py 2222 tmux "$@"
```

## How it works

```
client --TCP--> server --ssh stdin/stdout--> agent --> command
```

- The server listens on `127.0.0.1:PORT`, and starts `ssh DESTINATION
  python3 -c AGENT`, sending the code of the agent as the command.
  Nothing is installed on the host.
- Each client has a TCP connection of its own.  All the commands are
  multiplexed through the ssh connection by the id of their streams.
- The agent runs each command by asyncio, feeding its stdin and
  reading its stdout and stderr as they come.
- If the ssh connection exits, the commands running fail with 255, and
  the next command starts it again.

Both links carry the same frames:

```
type (1 byte), stream id (4 bytes), length (4 bytes), payload
```

| Type   | Direction       | Payload                                    |
|--------|-----------------|--------------------------------------------|
| OPEN   | client -> agent | JSON of the args, with the token from the client |
| STDIN  | client -> agent | data                                       |
| EOF    | client -> agent | none, the end of stdin                     |
| KILL   | client -> agent | signal number, sent if the client is gone  |
| STDOUT | agent -> client | data                                       |
| STDERR | agent -> client | data                                       |
| EXIT   | agent -> client | exit code, 128 + signal if signaled        |

The stream id is 0 between the client and the server.  Integers are
big endian.

## Files

In `~/.ssh-cli-proxy/`:

- `PORT.token`: the token of the server, readable by the user only.
  A client must send it, so other users and web pages can't run
  commands through the port.  It is removed when the server stops.
- `PORT.log`: the log of the server, also on its stderr: the ssh
  connection, and each command with its args, exit code, time and the
  sizes of its data, but not the data.

## Limitations

- No terminal: the command has pipes, not a pty, so full screen
  applications don't work.
- No flow control per command: a client reading slowly delays the
  output of the others through the same connection.
- If the agent is killed, not disconnected, the commands running are
  left on the host.
