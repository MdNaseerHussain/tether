# tether — Architecture

This document is the implementation contract for the packages under `internal/` and
`cmd/`. If code and this document disagree, this document wins until it is revised
in the same change.

## 0. Implementation status (v0.1.0)

Implemented: PTY allocation and the raw-mode client engine, the sequence-numbered
ring buffer with exclusive-cursor replay, the rate-limited reconnect loop with
exponential backoff and jitter, detach, `tetherd list`/`kill`, and cleanup of the
socket, pid file and metadata on normal exit, `SIGTERM` and protocol `Kill`.

Not implemented yet (designed below, do not assume): the client-side bootstrap in
§11 (there is **no** SFTP/SCP upload, so `tetherd` must already be on the remote
host), `~/.ssh/config` parsing (§6/§7 — pass `-i`, `-p`, and a literal host), the
`tether` SSH *subsystem* channel (the client currently execs
`tetherd attach --session <id> --spawn`), and multi-device attach as a supported
feature (the daemon has no "one client" lock, but the UX is untested).

## 1. Goals and non-goals

**Goals**

- A remote interactive shell (or AI CLI agent) survives client disconnects, laptop
  sleep, and Wi-Fi handoffs without losing a single byte of streamed output.
- Transport is standard SSH on port 22. No UDP, no extra listening ports, no
  firewall tickets, no root.
- The terminal experience is invisible: no status bar, no chrome, no keybinding
  layer. Native scrollback, mouse selection, OSC 52 clipboard, UTF-8 and
  TrueColor all pass through unmodified.

**Non-goals**

- Multi-user sessions, session sharing, or access control beyond "same Unix user".
- Rendering the screen on the server (no terminal emulator state, no screen diffing).
- Transparent renegotiation of TCP itself; tether reconnects at the application
  layer and replays bytes.
- Windows support in this revision.

## 2. Components

```
 ┌────────────────────────────── Local machine ──────────────────────────────┐
 │                                                                           │
 │  terminal emulator (Alacritty / iTerm2 / Kitty / Ghostty)                  │
 │        │  raw-mode bytes (UTF-8, CSI, OSC 52, mouse reports)               │
 │        ▼                                                                  │
 │  cmd/tether ── internal/client                                            │
 │    ├─ raw mode + signal router (SIGWINCH, SIGINT/SIGQUIT as data)          │
 │    ├─ detach parser (`~.` at start of line)                                │
 │    ├─ SSH transport (golang.org/x/crypto/ssh)                              │
 │    ├─ heartbeat (Ping/Pong)                                                │
 │    └─ reconnect loop (backoff, replay from LastAckSeq)                     │
 │        │                                                                  │
 └────────┼──────────────────────────────────────────────────────────────────┘
          │  TCP :22  ── SSH user auth (agent / key / password) ── exec channel
          │  running `tetherd attach`: framed proto (docs/PROTOCOL.md)
          ▼
 ┌────────────────────────────── Remote host ────────────────────────────────┐
 │  sshd ──> tetherd attach  (bridge; lives exactly as long as the channel)  │
 │              │                                                            │
 │              │ connect(2) to ~/.tether/sessions/<id>.sock (0600)          │
 │              ▼                                                            │
 │  tetherd session process (setsid, detached, one per session)               │
 │    ├─ socket server (unix listener, one attach at a time + waiting queue)  │
 │    ├─ ring buffer (4 MiB, seq-numbered)  <── PTY master read pump          │
 │    ├─ PTY master  ──────────────────────> child shell / Claude CLI         │
 │    └─ metadata: id, pid, created, rows/cols                               │
 │                                                                           │
 └───────────────────────────────────────────────────────────────────────────┘
```

Process roles:

| Process | Lifetime | Responsibility |
| :-- | :-- | :-- |
| `tether` (client) | one per interactive session | raw mode, SSH dial, framing, replay, detach |
| `tetherd serve` | one per SSH channel, short lived | bridge between the SSH channel and the session socket |
| session supervisor | one per session, detached (`setsid`) | owns the PTY, the ring buffer, and the socket listener |
| child shell | until it exits or the session is killed | the user's actual work |

Separating the supervisor from the SSH-spawned process is what makes survival
possible: sshd closing the channel kills `tetherd serve`, not the session.

## 3. Session lifecycle

### 3.1 Creation

`tether host` (or an explicit `tetherd attach <id>`) resolves a session for the
current directory + user, and if none exists asks the daemon to create one:

1. Ensure `$TETHER_HOME/sessions` exists with mode `0700` (`TETHER_HOME` defaults
   to `$HOME/.tether`).
2. Generate a session id: 8 lowercase base32 characters, plus an optional
   user-supplied name.
3. Create/truncate `$TETHER_HOME/sessions/<id>.log` (mode `0600`).
4. Fork a supervisor process with `setsid(2)` so it leaves the SSH session's
   process group and has no controlling terminal. Its own stdin/stdout/stderr are
   redirected to `<id>.log`, which keeps panics and diagnostics even when nobody
   is attached. The *child shell* never writes to that file — it gets the PTY.
5. The supervisor allocates a PTY pair (`creack/pty`), starts the shell with
   `Sys: &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: <slave fd>}`,
   `TERM` inherited from the hello frame's environment, and the slave as stdin,
   stdout and stderr.
6. The supervisor listens on `$TETHER_HOME/sessions/<id>.sock` with mode `0600`,
   then writes `<id>.json` metadata (`pid`, `created`, `rows`, `cols`, `cwd`).
7. The supervisor closes its copy of the slave fd and starts the read pump that
   copies PTY master → ring buffer.

Only after the socket exists does the daemon report success, so a client that
receives a session id can immediately attach.

### 3.2 Attach

`tetherd serve` connects to `<id>.sock` (or the newest/most recently used socket
when no id is given), sends a `TypeHello`, and becomes a dumb relay between that
Unix socket and stdin/stdout. The supervisor validates peer credentials (§9) and
admits one client at a time; additional clients wait in a queue and receive a
`TypeError` ("session busy") after a short timeout.

### 3.3 Exit and cleanup

When the child shell exits, the supervisor:

1. Pushes the last PTY bytes into the ring buffer.
2. Sends `TypeExit` with the child's exit status to the attached client (if any).
3. Waits a short grace period (2 s) for the client to read it, then unlinks
   `<id>.sock` and `<id>.json`.
4. Keeps `<id>.log` for post-mortem inspection; it is rotated/deleted by
   `tetherd list --prune` or when the user runs `tetherd kill <id>`.

`TypeKill` (client → server) signals the child's process group with `SIGTERM`,
waits 5 s, then `SIGKILL`, and removes the session.

### 3.4 Discovery

`tetherd list` enumerates `$TETHER_HOME/sessions/*.json` and pings each socket, so
stale metadata from a crashed supervisor is reported as `dead` and can be pruned.

## 4. Why attach is a dumb byte bridge

The supervisor does **no** terminal emulation. It never interprets CSI/OSC
sequences, never tracks a cursor, and never re-renders a screen. Its client side
is a `copy` loop over the socket and the client's is a `copy` loop over the SSH
channel.

Consequences, all of which are requirements:

- **OSC 52 works.** Clipboard sequences are opaque bytes, so the local terminal
  performs the copy exactly as it would over plain SSH.
- **UTF-8 and TrueColor work.** No decoding means no re-encoding, so split
  multi-byte sequences across frames are harmless: the local terminal reassembles
  them.
- **Native scrollback works.** The client writes bytes to its own tty; the
  emulator's scrollback is the emulator's business.
- **Multiple client terminals work.** Anything that renders the same byte stream
  looks the same.

The only state the supervisor keeps is the byte stream, its sequence numbers, and
the PTY window size.

## 5. Sequence numbers and replay

Every byte the PTY produces is assigned a 1-based sequence number by
`internal/ringbuf`. The buffer retains the most recent 4 MiB (configurable per
session, `DefaultCapacity`) and never blocks the PTY pump: when it is full, the
oldest bytes are dropped and `Dropped()` grows.

Wire behaviour:

1. The client tracks `lastAckSeq`, the highest sequence number it has written to
   its terminal and flushed.
2. On attach the client sends `TypeHello{Version, LastAckSeq, Rows, Cols}`.
   `LastAckSeq == 0` means "I have nothing; give me whatever you still have".
3. The supervisor calls `ring.ReadFrom(lastAckSeq)`, which returns the bytes
   *after* that cursor, `startSeq` (the sequence of the first returned byte) and
   an `overrun` flag.
   - No overrun: the supervisor sends them as `TypeData{firstSeq: startSeq}`.
   - Overrun: the first frame is `TypeOverrun{fromSeq, toSeq}` covering the lost
     range `[lastAckSeq+1, startSeq)`, and the client prints
     `ringbuf.NoticeOverrun` before rendering the replay. The range is
     half-open, so `toSeq == startSeq` of the following data frame.
4. While attached, the supervisor streams every new PTY read as a `TypeData`
   frame as soon as it arrives (coalescing reads up to 1 MiB, one frame per
   read). `firstSeq` disambiguates gaps, so a client can detect loss even
   without an explicit overrun frame.
5. The client acknowledges progress with `TypeAck{seq}` — at most every 100 ms
   and always when it has no unacknowledged bytes left, to keep the ack fresh
   without flooding the channel.
6. On reconnect the sequence repeats from step 2. Because sequence numbers are
   monotonic for the life of the session, replay is idempotent: re-sending bytes
   the client already discarded (e.g. after a lost Ack) restarts exactly at the
   client's cursor.

`ReadFrom` semantics that callers depend on:

```go
data, startSeq, overrun := buf.ReadFrom(lastAckSeq)
// len(data) == 0 when lastAckSeq == buf.Head()
// startSeq   == lastAckSeq+1 normally, or the oldest retained seq after clamping
// overrun    == lastAckSeq+1 < buf.OldestSeq()  (or lastAckSeq > buf.Head())
```

The ring buffer is the only unbounded-growth risk in the system, and it is
bounded by construction at `capacity` bytes. There is no per-client queue.

## 6. Heartbeat

- Both sides send `TypePing` every 3 s while a session is attached.
- Any received `TypePing` is answered immediately with `TypePong` carrying no
  payload; `TypePing`/`TypePong` may arrive from either side.
- If 9 s (three intervals) pass without any inbound frame — Pong or otherwise —
  the connection is declared dead, the socket is closed, and the client enters
  the reconnect loop. The server simply drops the attachment and keeps the
  session.
- Heartbeats are not a replacement for TCP keepalive: they also detect a live TCP
  socket attached to a wedged peer (suspended laptop, frozen VM).

## 7. Reconnect and backoff

While the terminal is in raw mode the client retries forever:

| Attempt | Delay |
| :-- | :-- |
| 1 | 500 ms |
| 2 | 1 s |
| 3 | 2 s |
| 4 | 4 s |
| 5+ | 5 s (cap) |

Every delay is jittered by ±20 % (`d * (0.8 + 0.4*rand.Float64())`). The delay
resets to 500 ms after a successful attach that survives one heartbeat interval.

Reconnect re-dials SSH using the user's ssh-agent and identity files
(`SSH_AUTH_SOCK`, `-i`, or `~/.ssh/id_*`) and re-attaches to the same session id.
`~/.ssh/config` is not parsed, so use a literal host plus `-i`/`-p`. If the host is
unreachable the loop continues indefinitely; it never exits on its own while
disconnected.

**Escape hatch:** two `Ctrl+C` presses within 2 s *while disconnected* abort the
retry loop and restore the terminal. `Ctrl+C` while connected is ordinary data
(§8), so this cannot fire during a live session: the client only counts keys
locally when there is no channel to send them to.

`TypeDetach` is different from a disconnect: it is a deliberate, clean exit.

## 8. Detach, raw mode and signals

### 8.1 Detach

- Default detach sequence: `~` followed by `.`, typed at the **start of a line**
  (i.e. immediately after pressing `Enter`), mirroring OpenSSH. `~~` at the start
  of a line sends a literal `~`.
- On match the client sends `TypeDetach`, waits for the daemon to close the
  channel (or 500 ms), restores the terminal, and exits 0. The session keeps
  running; `tether host` re-attaches to it later.
- The sequence is configurable (`--escape`), and can be disabled with
  `--escape none`.

### 8.2 Raw mode

The client puts the tty into raw mode with `golang.org/x/term.MakeRaw`, which
clears `ISIG`, `ICANON` and `ECHO` and disables output post-processing. Therefore:

- `Ctrl+C`, `Ctrl+Z`, `Ctrl+\` and `Ctrl+D` are **bytes** (`0x03`, `0x1a`, `0x1c`,
  `0x04`) forwarded inside `TypeData` frames. The remote PTY line discipline
  interprets them, so job control and interrupts work exactly as over SSH.
- There is no local signal handler for `SIGINT`/`SIGQUIT`; the client only handles
  `SIGWINCH`, `SIGTERM`/`SIGHUP` (restore terminal and detach) and `SIGCONT`.
- Restoring the terminal is deferred on every exit path, including panics and
  failed attaches; a crash must not leave the user with a raw tty.
- Because `OPOST` is off locally, the client must not translate `\n`; it writes
  bytes verbatim.

### 8.3 Resizing

`SIGWINCH` → `term.GetSize(os.Stdout.Fd())` → `TypeResize{Rows, Cols}`. The
supervisor applies it with `pty.Setsize(master, &pty.Winsize{Rows, Cols})`, then
sends `SIGWINCH` to the child process group. The size from `TypeHello` is applied
before the first byte is replayed, so full-screen programs see the right geometry
for their redraw.

## 9. Paths, environment, security

```
$TETHER_HOME/                     (default $HOME/.tether, mode 0700)
├── sessions/
│   ├── <id>.sock                 0600, unix listener owned by the session
│   ├── <id>.json                 metadata: pid, created, rows, cols, cwd
│   └── <id>.log                  0600, supervisor stdio / crash output
└── bin/                          optional staged binaries
```

- `TETHER_HOME` overrides the root for both binaries and is honoured by
  `tetherd` on the remote side (the client forwards it through the SSH command
  line, not the environment, since `AcceptEnv` is rarely configured).
- `TETHER_DEFAULT_SHELL` overrides `$SHELL` for new sessions.
- `TETHER_RINGBUF_BYTES` overrides the default 4 MiB retention per session.
- Optional bootstrap target: `~/.local/bin/tetherd` (§11).

Security properties:

- **No network listener.** The only sockets are Unix domain sockets under
  `$TETHER_HOME/sessions`; the transport is the user's own sshd.
- **File permissions are the authentication.** Directories are `0700`, sockets and
  logs are `0600`. Before accepting a client the supervisor checks
  `SO_PEERCRED` (`syscall.GetsockoptUcred` on Linux, `LOCAL_PEERCRED` on darwin)
  and compares the peer uid with its own; a mismatched or unobtainable uid is
  rejected and audited in the log. Same-uid is the entire trust model: the peer
  can already attach to the user's terminal by other means.
- **No privileged operations.** No setuid, no root, no system units. `tetherd`
  runs entirely in user space.
- **The daemon never executes a client-supplied command.** It starts `$SHELL`
  (or the session's configured command) and nothing else; `TypeHello` carries no
  argv.
- **The log is untrusted output.** It can contain anything the shell printed,
  including escape sequences; never `cat` it into a terminal that matters.
- **Bootstrap** only stages a binary the user already trusts over the user's own
  authenticated SSH connection, with `chmod 0700`.

## 10. Policies

### 10.1 Slow clients

A client that cannot keep up must not make the server allocate.

- The supervisor never queues per-client output. It writes to the socket with the
  ring buffer as the only buffer; if the socket would block for more than 2 s the
  attachment is dropped (the session continues).
- On the next attach the client replays from its `LastAckSeq`, which gives it
  exactly what it missed while it was gone, plus `TypeOverrun` if it missed more
  than the retention window.
- The retention window is fixed at attach time; it never grows for a slow client.

### 10.2 Flow control in the other direction

Client → server input is small and human-paced. The client writes `TypeData`
frames synchronously and does not buffer keystrokes beyond one frame; there is no
input replay (a keystroke that never reached the PTY is simply lost, which is the
same behaviour as SSH).

### 10.3 Concurrency model (server)

```
PTY master ──read──> ring.Write  ──> attached socket (single writer goroutine)
socket ─────frames──> PTY master  (single reader goroutine)
```

Exactly one goroutine writes to the attached socket and one reads from it, so
frame boundaries are never interleaved. `proto.WriteMessage` emits a frame in a
single `Write` call but does not add locking; callers serialise.

### 10.4 Limits

| Limit | Value |
| :-- | :-- |
| Max frame payload | 1 MiB (`proto.MaxFrameSize`) |
| Ring retention | 4 MiB per session (`ringbuf.DefaultCapacity`) |
| Heartbeat interval / timeout | 3 s / 9 s |
| Reconnect backoff | 500 ms doubling to 5 s, ±20 % |
| Attach queue wait | 5 s, then `TypeError` |

## 11. Bootstrap (client-side) — planned, not in v0.1.0

If `tetherd version` fails on the remote host, `tether` will one day stage it; as
shipped today it fails with `tetherd not found on <host>; install tetherd or pass
--remote-bin <path>`. The design:

1. Detect remote `GOOS`/`GOARCH` (`uname -s`, `uname -m`).
2. Upload the matching embedded/prebuilt static binary to
   `~/.tether/bin/tetherd-<version>` over the existing SSH connection (SFTP
   subsystem, or `cat > file` when it is unavailable).
3. `chmod 0700`, atomically rename into place, then run `tetherd version` to
   verify. If verification fails, fall back to plain `ssh` with a clear warning.

## 12. Planned package layout

```
cmd/tether        local client CLI
cmd/tetherd       remote daemon CLI (serve | attach | list | kill | version)
internal/client   raw-mode terminal, SSH transport, reconnect loop, detach parser
internal/daemon   session, PTY, socket server, detached spawn, bridge
internal/proto    framed wire protocol
internal/ringbuf  seq-numbered circular buffer
internal/version  build version and protocol version
```

`creack/pty` is used directly; there is no `internal/pty` wrapper.

Dependency direction is strictly one-way: `cmd/*` → `internal/{client,daemon}` →
`internal/{proto,ringbuf,version}`. `proto` never imports `ringbuf`; sequence
numbers travel as plain `uint64` fields.
