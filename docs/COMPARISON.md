# tether vs. the alternatives

This document compares `tether` with the tools people already use to keep a remote
shell alive: plain OpenSSH, Mosh, Eternal Terminal (ET), and a terminal multiplexer
(`tmux`/`screen`) under SSH.

**Evaluation lens.** Not "a command I ran once", but an *interactive, long-running,
streaming* session on a remote dev box (AWS Cloud Desktop, GCP Cloudtop, EC2/GCE)
driven from a laptop that sleeps, roams networks, and sits behind a corporate
firewall — canonically a CLI AI agent (Claude Code, Aider, Goose) mid-generation for
2–15 minutes while the lid is shut, then a Wi-Fi handoff on wake.

Two questions separate the tools: **does the child process survive the disconnect**
(process persistence), and **do the bytes emitted while you were away still reach
your terminal** (transport persistence with replay)? Most tools answer one. Almost
none answer both without a cost in ports, privileges, clipboard, or scrollback.

---

## TL;DR — when to use what

| Tool | Best for | Main trade-off |
| :-- | :-- | :-- |
| **Standard SSH** | The default. Interactive work on a stable network, where the client never disconnects. | Zero tolerance for disconnect: TCP death → `SIGHUP` → the agent dies and the stream is lost. No replay. |
| **Mosh** | Roaming laptops on networks that permit UDP; lossy, high-latency links where local echo helps. | Syncs *screen state*, not bytes: no scrollback, OSC 52 breaks, needs UDP 60000–61000, no TCP fallback. |
| **Eternal Terminal** | Teams willing to run a root-installed daemon and open one extra TCP port for session survival. | Custom TCP **2022** must be reachable; root to install `etserver`; heavyweight C++/Boost/Protobuf stack. |
| **tmux / screen** | Many sessions, shared sessions, pane layouts, a process tree independent of the network. | Persists the *process*, not the *transport*: manual reattach, plus a keybinding layer and copy-mode. |
| **tether** | Long-running interactive sessions (especially streaming agents) on locked-down networks, with native terminal behaviour. | Requires `tetherd` on the remote host (auto-bootstrap is not in v0.1.0), in-memory session state (lost if the daemon dies), no persisted history. |

---

## 1. Standard SSH

**How it works.** `ssh host` opens TCP :22, authenticates, and requests a PTY with
`pty-req`. `sshd` forks a child, allocates a PTY pair, and execs the login shell on
the slave side. The channel is then a byte pipe between your terminal and the PTY
master. The session exists entirely inside `sshd`'s process tree.

**Strengths.** Already installed, already authenticated, already permitted by every
firewall and bastion. Perfectly transparent: nothing in the middle interprets escape
sequences, so OSC 52, TrueColor, mouse reporting, and native scrollback behave
exactly as your emulator implements them. Nothing to install remotely.

**Why it does not survive a lid close.** SSH has no session object of its own. The
PTY exists because an `sshd` child holds it open; when the TCP connection dies,
`sshd` closes the master, revokes the slave, and sends `SIGHUP` to the session
leader. The foreground process group — your shell, and inside it Claude Code —
receives `SIGHUP` and, on its next write, `EIO` on the pty slave (or `EPIPE` if the
output path is a pipe). There is no buffer, no owner, and no replay: bytes already
written into a discarded pty buffer are gone with it.

**Friction.** No ports beyond 22; no root; native clipboard and scrollback; no
keybindings. **But there is no reconnect** — TCP keepalive defaults are measured in
hours (`net.ipv4.tcp_keepalive_time` is 7200 s) and `ClientAliveInterval` is disabled
in stock `sshd_config`, so a slept laptop often leaves a session that looks alive and
is actually undeliverable until a write draws a RST. Multi-attach is N/A; each
connection is its own session.

---

## 2. Mosh

**How it works.** `mosh-client` (local) bootstraps over SSH: it runs `mosh-server
new` through an exec channel, reads back the session key and the UDP port the server
picked (**60000–61000**), then **closes SSH** and speaks UDP only. Inside that flow
it runs **SSP**, the State Synchronization Protocol. Both ends keep a terminal
emulator model: `mosh-server` reads the PTY and applies output to *its* model, and
the client applies the resulting diffs to its own. Nothing raw crosses the wire —
what is transmitted is a description of the visible screen plus a predictive-echo
overlay for local keystrokes.

**Strengths.** Genuinely resilient to roaming: the client's IP and UDP source port
may change freely and the server accepts authenticated packets from a new address,
so a Wi-Fi handoff recovers in seconds without a new login. Local echo keeps typing
usable at 200 ms RTT. `mosh-server` keeps draining the PTY while you are gone, so the
child never blocks on a full pty buffer and never sees `SIGHUP`.

**Why it does not solve streaming output.** Mosh's durability is *screen* durability,
and this is the part people routinely misread as "it keeps my output". The server
model holds one screen. So:

1. **Tokens that scrolled off while you were disconnected are gone.** If Claude Code
   emitted 2 minutes of output, the model ended at the last screenful; intermediate
   lines were overwritten inside the model and never existed anywhere else. On wake
   the client repaints the *current* screen, not the stream.
2. **Your local scrollback is no longer a transcript.** The client writes screen
   diffs, not bytes, so scrollback accumulates repaints and cursor jumps.

The model also drops sequences it does not understand — which is why OSC 52 does not
survive: there is no clipboard concept in the model to forward.

**Friction.** UDP **60000–61000** must be open inbound on the remote host — the usual
reason Mosh is unusable at a corporation, since many VPN concentrators and security
groups drop UDP and Mosh has **no TCP fallback**. No root to run, but `mosh-server`
must be installed remotely, which on a locked-down image is a ticket. Clipboard and
scrollback are broken per above. Keybindings are minimal (`Ctrl+^` to quit).
Reconnect is driven by SSP retransmission on packet loss, not an explicit heartbeat;
one client per `mosh-server`.

---

## 3. Eternal Terminal (ET)

**How it works.** ET replaces the SSH session with its own protocol. `etserver` is a
**daemon** installed on the remote host (distro package, Homebrew, or `make install`)
and normally registered as a system service — hence root. `et` connects over **TCP
2022**, authenticates, and asks for a session id. `etserver` allocates the PTY and
keeps it alive independently of the client connection; output is appended to a
per-session replay buffer, and a reconnecting client is sent what it missed.

**Strengths.** Session survival with replay, conceptually close to `tether`. TCP, so
it works on networks that block UDP — provided 2022 is permitted. Roaming and
reconnect without a fresh interactive login.

**Why it does not fully solve the streaming case.** ET's replay is a terminal-buffer
replay owned by the daemon, not a session-long, sequence-numbered byte stream. On
reconnect you get the retained tail of that buffer, and how much is retained is a
server-side policy you do not control from the client. Outlast the buffer and the
beginning of the missed output is silently absent — there is no equivalent of an
explicit "bytes `[fromSeq, toSeq)` were dropped" marker.

The structural problems are elsewhere. **Port 2022 is a new hole**: corporate egress
filters commonly allow 22 and 443 only, and when 2022 is blocked you do not get a
degraded session, you get none. **It is a root daemon**: installing or upgrading
`etserver` needs root on every host, which on ephemeral cloud dev boxes is often
impossible. And the dependency tree (C++, Boost, Protobuf, and depending on build
SSL) is large for what is a byte relay, which makes packaging lag new distros.

**Friction.** TCP 2022 inbound, explicitly. Root for daemon install/service. No
clipboard management: OSC 52 is proxied in normal operation, but the replay path
makes behaviour version- and emulator-dependent. Scrollback stays local, though
replayed buffer boundaries insert discontinuities. A small set of `et`-specific
escapes; one client; reconnect via TCP failure detection plus a retry loop.

---

## 4. tmux / screen

**How it works.** `tmux new -s work` starts a **server** that `setsid`s away from your
login session, allocates a PTY per pane, and runs your shell inside it. The `tmux`
binary you typed is only a *client*: it connects to the server over a Unix socket
(`/tmp/tmux-<uid>/`) and passes bytes through. That separation is exactly why tmux
survives a disconnect — the client dies with SSH, the server keeps the pane's PTY
master open, keeps reading it, and keeps parsing output into its own grid.

**Strengths.** Rock-solid process persistence, twenty years of hardening. Real
scrollback (default `history-limit` 2000 lines, configurable). Multiple clients,
shared sessions, pairing, panes, scripting.

**Why "persistence" here is not what the AI-agent case needs.** tmux separates
**process persistence** from **transport persistence**, and only promises the first:

```
 your terminal ── ssh ──> sshd ──> tmux client ══socket══> tmux server ──PTY──> claude
      ▲                              │                        │
      │                              └── dies with the TCP conn└── survives, keeps draining
      └── on wake you must open a *new* ssh and run `tmux attach`
```

The child never notices the outage — good. But *you* do: no automatic reattach, no
knowledge of which session you were in, no continuity of the byte stream into your
terminal. Your terminal froze at the last painted frame; on reattach tmux repaints its
current screen, output past `history-limit` is already dropped, and everything
produced while you were away exists only as grid state, not as a stream your emulator
can scroll through. For a 2-minute Claude turn you get the *end* of it inside
copy-mode — if you know to press `Ctrl+B [`.

**Friction.** No ports beyond SSH, which is tmux's real advantage, and no root. The
rest is cost: **clipboard** is the big one — tmux intercepts mouse selection and
OSC 52, and native copy/paste needs `set-clipboard on`, matching `terminal-features`,
and often surrendering the mouse to the application. **Scrollback** is owned by tmux
(`copy-mode`), not your terminal. **Keybindings**: a prefix (`Ctrl+B`) that collides
with readline, Emacs, Vim, and TUI apps, plus altered mouse selection and the
daily `Ctrl+B Ctrl+B` papercut. Reattachment is manual; multi-attach works (genuinely
useful for pairing, and something `tether` does not yet do).

---

## 5. Failure-mode walkthrough

One scenario, traced through each tool.

```
 t = 0:00   Claude Code is mid-generation on the remote box, streaming tokens;
            ~2 minutes of the turn remain.
 t = 0:01   Lid closes. The machine sleeps. No FIN, no RST — the NIC just stops.
 t = 2:00   Lid opens on a different network (office → hotspot, new DHCP lease).
            Whatever TCP state existed is now unreachable.
```

### 5.1 Standard SSH

| t | What happens |
| :-- | :-- |
| 0:01 | Laptop sleeps, nothing is sent; `sshd` still sees a valid TCP socket. Claude keeps writing into the PTY master's line-discipline buffer (a few kilobytes), which fills fast, so `write(2)` on the slave **blocks** — stalled mid-stream, not dead. |
| 0:01–2:00 | `sshd` notices nothing: `ClientAliveInterval 0` (disabled) and TCP keepalive is ~2 hours away. With `ClientAliveInterval 60`, the session dies at 1:01 instead — worse, and silently. |
| 2:00 | On the new network the first outbound byte, or a retransmit, draws a RST or ICMP unreachable. `sshd` closes the PTY master and **sends `SIGHUP` to the session leader**, which forwards it to the foreground process group. Claude dies, seeing `EIO` on the slave if it wrote once more first. |
| on wake | A frozen terminal, then `Connection to host closed` or `broken pipe`. No replay is possible; the turn is gone and the prompt must be re-sent. |

### 5.2 Mosh

| t | What happens |
| :-- | :-- |
| 0:01 | Laptop sleeps. `mosh-server` keeps reading the PTY and applying output to its model; the child never blocks and never sees `SIGHUP`. |
| 0:01–2:00 | The model's screen is overwritten thousands of times. Only the current screen survives; scrolled-off lines are discarded server-side. SSP has no peer to send diffs to, so it sends nothing. |
| 2:00 | `mosh-client` wakes with a new source IP/port, sends an authenticated datagram, and the server updates the peer address after validating the session key. |
| on wake | Within a second or two the screen repaints to Claude's *current* state — cursor, status line, the last few lines. The intermediate tokens are unrecoverable by scrolling because they never existed as bytes locally. If the new Wi-Fi blocks UDP, nothing recovers at all. |

### 5.3 Eternal Terminal

| t | What happens |
| :-- | :-- |
| 0:01 | Laptop sleeps. `etserver` holds the PTY and appends output to the session's replay buffer; the child keeps running. |
| 0:01–2:00 | The buffer fills; at its limit the oldest data is lost, with no notice to the user about what went missing. |
| 2:00 | `et` wakes, the old TCP connection to :2022 is dead, and the client re-dials. If :2022 is blocked from the hotspot or guest Wi-Fi, this is where it ends. |
| on wake | A burst of replayed output, then live tokens — or a retry loop and no session, with the process alive but unreachable from this client. |

### 5.4 tmux over SSH

| t | What happens |
| :-- | :-- |
| 0:01 | Laptop sleeps. The SSH socket goes quiet, but the tmux **server** holds the pane PTY and keeps draining it into its grid; Claude never blocks. |
| 0:01–2:00 | Output accumulates in the grid and history (`history-limit`, default 2000 lines); older lines are evicted. |
| 2:00 | SSH dies exactly as in 5.1, but the damage stops at `sshd` and the tmux *client*; the `setsid`-detached server is unaffected. |
| on wake | A dead terminal window, nothing automatic. After `ssh host` and `tmux attach -t work` the session is intact, but you are looking at its end; the middle needs copy-mode, and anything past `history-limit` is gone. |

### 5.5 tether

- **0:01** Laptop sleeps. `tetherd`'s supervisor is `setsid`-detached and owns the PTY
  master and ring buffer. It keeps reading; Claude never blocks, never sees `SIGHUP`.
- **0:01+** Every byte is appended to the 4 MiB (default) sequence-numbered ring
  buffer. The client's last `Ack` — sent at most every 100 ms — marks the last byte it
  flushed to its terminal; everything after that cursor is queued for replay.
- **0:01–0:10** Heartbeats stop arriving. After 9 s with no inbound frame the client
  declares the connection dead and closes the socket; the server drops the
  attachment and keeps the session. This is a normal state, not an error.
- **0:01–2:00** The buffer fills toward 4 MiB. A typical turn stays well inside it; if
  it does not, it wraps and the drop is reported explicitly at replay time.
- **2:00** The client's reconnect loop has been running throughout — 500 ms → 1 s →
  2 s → 4 s → 5 s cap, ±20 % jitter, indefinitely. It re-dials SSH (agent/key from
  your existing setup), execs `tetherd attach --spawn` on an SSH exec channel, and sends
  `Hello{version, LastAckSeq, rows, cols}`.
- **2:00+** The supervisor calls `ReadFrom(lastAckSeq)` and streams the missed bytes as
  `Data{firstSeq}` frames, coalescing up to 1 MiB per read. `Resize` is applied before
  the first replayed byte, so a full-screen program sees the right geometry. If the
  cursor fell outside the retention window, `Overrun{fromSeq, toSeq}` precedes the
  replay and the client prints `[tether: output buffer overrun; display truncated]`
  once.
- **On wake:** the terminal fills with the tokens emitted while the lid was shut, in
  order, then live output continues. The replayed bytes are raw PTY output, so the
  emulator handles cursor movement, colour, and OSC 52 exactly as over plain SSH.
  No manual reattach, no keybinding.

One honest caveat: `tether` does not force a full-screen TUI to repaint on attach.
The replay ends where the daemon's output ended, so an app that was mid-frame when
the link dropped may look stale until it next writes; geometry is correct, and the
repaint is the application's business.

### 5.6 Wake-up summary

| Tool | Child alive? | Missed output delivered? | Manual step on wake? |
| :-- | :-- | :-- | :-- |
| Standard SSH | No — `SIGHUP` | No | Restart the agent |
| Mosh | Yes | Only the current screen | None (if UDP is permitted) |
| Eternal Terminal | Yes | Buffered tail, policy-bounded | None (if :2022 is permitted) |
| tmux / screen | Yes | Grid + `history-limit`, via copy-mode | `ssh` + `tmux attach` |
| tether | Yes | Yes — exact bytes from `LastAckSeq` | None |

---

## 6. Protocol and architecture comparison

| Attribute | Standard SSH | Mosh | Eternal Terminal | tmux / screen | tether |
| :-- | :-- | :-- | :-- | :-- | :-- |
| **Transport** | TCP :22 (SSH) | UDP, own SSP protocol after an SSH bootstrap | TCP :2022, custom protocol | SSH :22 carrying a tmux client protocol over a Unix socket | SSH :22 exec channel running `tetherd attach`, framed messages |
| **Port / firewall** | 22 only | 22 outbound + UDP **60000–61000** inbound | 22 for bootstrap + TCP **2022** inbound | 22 only | 22 only |
| **Persistence model** | None | Detached `mosh-server` holding the PTY | Root-installed `etserver` holding the PTY | Detached tmux server holding pane PTYs | `setsid`-detached supervisor per session holding the PTY |
| **Replay granularity** | None | Screen state (diffs of a terminal model) | Retained session buffer, oldest evicted | Grid + `history-limit` lines | Byte stream from `LastAckSeq`, with explicit `Overrun{from,to}` |
| **Clipboard (OSC 52)** | Native passthrough | Not preserved (screen model drops it) | Proxied, replay-dependent | Intercepted; needs `set-clipboard`/`terminal-features` | Opaque passthrough, both directions |
| **Scrollback owner** | Local emulator | Server model (no real history) | Local emulator + daemon buffer | tmux `copy-mode` | Local emulator |
| **Root required** | No | No to run; package install remotely | Yes, to install/run `etserver` | No | No — user space only, Unix sockets under `$TETHER_HOME` |
| **Binary / deps** | Base system | C++ client/server, protobuf, ncurses, SSL | C++/Boost/Protobuf | C, libevent, ncurses | Two static Go binaries, `CGO_ENABLED=0`, no cgo |
| **Reconnect trigger** | None (keepalive, hours) | SSP retransmission on loss | TCP failure + client retry | None (manual) | 3 s heartbeat, 9 s dead, backoff 500 ms → 5 s ±20 % |
| **Multi-attach** | N/A (per-connection) | One client | One client | Many clients | One client; a second queues 5 s then gets `Error("session busy")` |

Figures such as the 1.5 ms latency budget, <25 MB RSS, 4 MiB/32 MiB retention and
500 ms → 5 s backoff are PRD targets, not measured results.

---

## 7. What tether does not do

A comparison that only lists wins is a brochure.

- **It needs `tetherd` on the remote host.** Today you install it yourself
  (`make install`, or drop the static binary in `~/.local/bin`). Automatic
  bootstrap — detect `GOOS`/`GOARCH`, upload over SFTP, `chmod 0700`, verify with
  `tetherd version` — is designed and roadmapped but **not in v0.1.0**. Without the
  daemon there is no session; the client exits with a "tetherd not found" error
  (pass `--remote-bin` if it lives outside `$PATH`).
- **Session state is in memory.** The PTY, ring buffer, and sequence counters live in
  the supervisor. A daemon crash, OOM kill, or host reboot loses the shell and its
  buffer. `tetherd list` reports the stale metadata as `dead`; it cannot resurrect it.
  Persisting to disk would trade the <25 MB RSS budget and the no-data-at-rest
  property for durability.
- **The ring buffer bounds history.** Default 4 MiB, configurable with
  `--buffer-size`. Exceed it while detached and the oldest bytes are gone, with an inline
  notice at replay; a `yes` loop overruns 4 MiB in well under a second. Deliberate:
  an unbounded buffer is an unbounded memory bug.
- **Multi-device attach is not a supported feature yet.** The daemon fans output
  out to every attached client and each replays from its own cursor, but there is no
  coordination or UI for it, so it is not advertised. Use one machine at a time.
- **No `~/.ssh/config` parsing yet.** Aliases, `ProxyJump`, `HostName`, `Include`,
  and `Match` are not resolved; pass `-i`, `-p`, and a literal host. Reconnect does
  use the SSH agent and on-disk keys, so a passphrase-prompting key will stall the
  retry loop.
- **No Windows support.** POSIX PTYs only (`/dev/ptmx`, `creack/pty`) with
  `SO_PEERCRED`/`LOCAL_PEERCRED` peer checks. Linux and macOS are the targets.
- **No pane management, by design.** No splits, tabs, or layouts. Run tmux inside a
  `tether` session if you want them.
- **No terminal emulation on the server.** A feature (§8.2), but it means the daemon
  cannot answer "what is on screen", cannot compress by diffing, and cannot
  deduplicate repetitive output: bandwidth is proportional to bytes produced, not to
  visual change.

---

## 8. Design notes

### 8.1 `tetherd attach` is a dumb byte bridge

`tetherd serve` is spawned per SSH channel, connects to
`$TETHER_HOME/sessions/<id>.sock`, sends `Hello`, and becomes a `copy` loop in both
directions. It never parses escape sequences, tracks a cursor, or renders anything.

- **Correctness by construction.** Split UTF-8, split CSI/OSC sequences, mouse
  reports, and bracketed-paste markers are all just bytes. No parser means no parser
  bugs and no chance of silently swallowing a sequence it does not recognise — the
  exact failure mode that breaks OSC 52 under a screen model.
- **The bridge is disposable.** Because it holds no state, killing it (which is what
  `sshd` does to the channel) costs nothing; the session lives one `connect(2)` away
  in the supervisor.
- **Two small binaries instead of a protocol stack.** `internal/proto` is five-byte
  headers and big-endian integers; the only negotiation is `Hello.Version`.

The cost: the bridge cannot answer state questions, so server-side search and screen
diffing are out of reach; bandwidth stays proportional to bytes produced.

### 8.2 Byte-stream replay instead of screen-state sync

Mosh's core idea — a terminal model on both ends, synchronised — is elegant and cheap
on the wire. `tether` deliberately does the opposite: keep the raw PTY byte stream,
number every byte, replay from the client's cursor.

- **Native scrollback survives.** The client writes bytes to its own tty, so the
  emulator's scrollback is a faithful transcript, not a record of repaints. You can
  scroll back through a 2-minute agent turn and read all of it.
- **OSC 52 survives.** A clipboard sequence is opaque bytes in `TypeData`; the local
  terminal performs the copy. No intermediate model decides whether it is meaningful.
- **TrueColor and UTF-8 survive** for the same reason: no decode, no re-encode, no
  normalisation.
- **Replay is idempotent.** Sequence numbers are monotonic for the life of the
  session, so a duplicate `Data` frame after a lost `Ack` is harmless: `ReadFrom`
  restarts exactly at the client's cursor. The only ambiguity is overrun, and that is
  explicit (`Overrun{fromSeq, toSeq}`) rather than inferred.

The cost: the buffer must be bounded in bytes, so long disconnects truncate instead
of compressing, and reattaching a full-screen TUI does not force a repaint.

### 8.3 One supervisor per session, `setsid`-detached

There is no global daemon. Each session is a supervisor that calls `setsid(2)`, gives
up its controlling terminal, redirects its own stdio to `<id>.log`, allocates the
PTY, and listens on `<id>.sock` (mode `0600`).

- **Survival is a process-tree property.** `sshd`'s teardown kills its own children; a
  process in a different session and process group is not in that set. The same
  mechanism tmux uses.
- **No root, no unit files, no PID 1 duties.** Nothing listens on the network: the
  only sockets are Unix domain sockets authenticated by `SO_PEERCRED` uid equality and
  `0700`/`0600` permissions. The transport is the user's own `sshd`.
- **Failure domains are per-session.** One wedged agent cannot take down another
  session, and `tetherd kill <id>` is a local `SIGTERM` to a process group rather than
  a daemon-wide operation.
- **Clean lifecycle.** On child exit the supervisor flushes the last bytes, sends
  `Exit{code}`, waits 2 s for delivery, then unlinks the socket and metadata.

The cost: N processes for N sessions, discovery by scanning `<id>.json` and pinging
sockets (`tetherd list` reports crashed supervisors as `dead`), and no shared state.

---

## 9. Further reading

- [Architecture](ARCHITECTURE.md) — components, session lifecycle, replay and
  heartbeat semantics, security model, limits.
- [Wire protocol](PROTOCOL.md) — frame layout, message types, the `Hello` / `Data` /
  `Ack` / `Overrun` handshake, hex examples.
- [Product requirements](PRD.md) — problem statement, personas, functional and
  non-functional requirements, MVP acceptance criteria.
- [README](../README.md) — install, quickstart, and the short comparison table.
