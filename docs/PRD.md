# Product Requirements Document (PRD)

## Project: `tether` — Persistent Remote Terminal Proxy
**Author:** Naseer Hussain  
**Status:** Draft / Active Review  
**Target Release:** v0.1.0 (MVP)  
**Language / Stack:** Go (1.22+), POSIX Unix PTY, SSH Protocol  

---

### 1. Executive Summary & Problem Definition

#### 1.1 The Problem
Engineers working on remote cloud environments (e.g., AWS Cloud Desktops, GCP Cloudtop, remote EC2/GCE instances) experience severe workflow disruptions when client machines sleep, roam across networks, or experience transient Wi-Fi drops.

With the rapid emergence of **interactive, streaming CLI AI developer tools** (e.g., Anthropic's Claude Code, Aider, Goose), shell sessions are frequently engaged in multi-minute, autonomous generation loops. Under standard SSH:
1. TCP timeouts or network interface handoffs cause immediate connection termination (`broken pipe`).
2. The remote shell sends `SIGHUP` or the active process receives `EPIPE` when attempting to write to standard output, terminating active agent runs mid-execution.
3. Any streamed tokens emitted while the connection is dropping are permanently lost.

#### 1.2 Limitations of Existing Solutions
* **Mosh:** Uses UDP state synchronization (`SSP`). It requires opening firewall ports (`60000–61000`), lacks native terminal scrollback buffers, strips OSC 52 clipboard sequences, and fails on corporate VPNs where UDP is blocked.
* **Eternal Terminal (ET):** Uses a custom TCP port (`2022`), requires root installation on the server, has a heavy C++ dependency tree, and suffers from maintenance lag.
* **`tmux`:** Solves server process persistence but does not prevent the SSH disconnection itself. It introduces invasive UI layers, intercepts mouse scrolling, breaks native OS clipboard copying, and requires manual re-attachment.

#### 1.3 Proposed Solution
`tether` is a lightweight, zero-configuration, single-binary Go application providing **transparent connection persistence over standard SSH (Port 22)**. It decouples the client terminal from the remote process lifecycle through a server-side daemon (`tetherd`) equipped with an in-memory stream buffer, native OSC 52 passthrough, and an automated reconnection state machine.

---

### 2. User Personas & Core Scenarios

#### 2.1 Target Persona
* **Software Development Engineers (SDEs):** Engineers who develop inside remote Linux environments from laptops (macOS / Linux), frequently closing their laptops to walk between desks, commute, or attend meetings.
* **AI-Augmented Developers:** Engineers who run autonomous CLI agents to execute complex refactorings, run test suites, or generate diffs that span 2 to 15 minutes per turn.

#### 2.2 Core User Stories
* **Story 1 (Laptop Sleep during AI Turn):** As a developer running Claude Code on my Cloudtop, I want to close my laptop lid during a generation turn and walk to a meeting room, so that when I open my laptop, the generation has continued uninterrupted and all output is displayed on my screen.
* **Story 2 (Native Clipboard Experience):** As a developer, I want to highlight text with my mouse or use Vim's clipboard copy inside a persistent remote session and have it immediately available in my local OS clipboard (`Cmd+V` / `Ctrl+V`) without configuring tmux clipboard hacks.
* **Story 3 (Zero Network Infrastructure Changes):** As an enterprise engineer behind strict corporate firewalls, I want connection resilience using only standard port 22 SSH authentication, without filing tickets to open high UDP port ranges.

---

### 3. Non-Goals (Scope Boundaries)

* **Not a GUI Terminal Emulator:** `tether` will not implement a graphical UI. It runs inside existing terminal emulators (iTerm2, Alacritty, Ghostty, Kitty, Terminal.app).
* **Not a Window Multiplexer:** `tether` will not provide split panes, tabs, or multi-window layouts. Users desiring pane management can run their tool of choice inside `tether`.
* **Not a Proprietary Network VPN:** `tether` does not create network tunnels or virtual interfaces (TUN/TAP). It operates strictly at the PTY and application stream level.

---

### 4. Technical Architecture & System Components

```
┌────────────────────────────────────────────────────────────────────────┐
│ LOCAL CLIENT (`tether`)                                                │
│                                                                        │
│  ┌────────────────────┐    ┌─────────────────────┐    ┌─────────────┐  │
│  │ Local Raw Terminal │<-->│ Reconnection Machine│<-->│ SSH Client  │  │
│  │ (termios / signals)│    │ (Backoff, Auth, Sync│    │ (Port 22)   │  │
│  └────────────────────┘    └─────────────────────┘    └──────┬──────┘  │
└──────────────────────────────────────────────────────────────┼─────────┘
                                                               │ SSH Tunnel
                                                               │ (Port 22)
┌──────────────────────────────────────────────────────────────┼─────────┐
│ REMOTE SERVER (`tetherd`)                                    ▼         │
│  ┌────────────────────┐    ┌─────────────────────┐    ┌─────────────┐  │
│  │ Pseudo-Terminal    │<-->│ Ring Buffer         │<-->│ Session     │  │
│  │ (PTY Master/Slave) │    │ (4MB Memory Log)    │    │ Protocol    │  │
│  └──────────┬─────────┘    └─────────────────────┘    └─────────────┘  │
│             ▼                                                          │
│  ┌────────────────────┐                                                │
│  │ Child Shell / Job  │ (bash/zsh, Claude Code, Aider, Compilers)      │
│  └────────────────────┘                                                │
└────────────────────────────────────────────────────────────────────────┘
```

#### 4.1 Component Breakdown
1. **Client (`tether`):**
   * Sets local stdin to raw mode (`golang.org/x/term`).
   * Listens for local `SIGWINCH` signals and forwards terminal dimensions to the remote PTY.
   * Manages the outbound SSH connection pool and reconnection state machine.
2. **Server Daemon (`tetherd`):**
   * Allocates a POSIX pseudo-terminal (`/dev/ptmx` via `creack/pty`).
   * Forks the user's login shell into the slave PTY.
   * Maintains session metadata in `~/.tether/sessions/<session_id>.sock`.
3. **Circular Ring Buffer:**
   * Pre-allocated in-memory circular buffer (default: 4 MB, configurable up to 32 MB).
   * Stores raw ANSI byte streams emitted by the PTY slave.
   * Tracks monotonically increasing byte sequence numbers (`SeqNum`) to enable lossless replay upon client re-attach.

---

### 5. Functional Requirements & Specifications

#### 5.1 Connection Resiliency & Reconnection State Machine
* **FR-1.1 (Heartbeat Detection):** The client and daemon shall exchange bidirectional heartbeat frames every 3 seconds. If 3 consecutive heartbeats fail (9 seconds) or an OS network interface event reports disconnect, the client transitions to `RECONNECTING` state.
* **FR-1.2 (Exponential Backoff):** Reconnection attempts shall use exponential backoff with jitter: initial interval 500ms, doubling up to a maximum interval of 5 seconds, running indefinitely until user cancellation (`Ctrl+C` twice) or successful connection.
* **FR-1.3 (Zero-Authentication Reconnect):** Reconnection shall authenticate using mutual session tokens generated during the initial SSH handshake, avoiding repeated interactive passphrase prompts.

#### 5.2 Stream Buffering and Replay
* **FR-2.1 (Non-Blocking Output):** When disconnected, the daemon PTY output must continue writing to the circular ring buffer without blocking child processes.
* **FR-2.2 (Lossless Replay Handshake):** Upon reconnection, the client sends `LastAckSeqNum`. The daemon streams all bytes from `LastAckSeqNum` up to current `HeadSeqNum`.
* **FR-2.3 (Buffer Overrun Handling):** If a child process emits more data while disconnected than the ring buffer capacity (e.g. >4 MB), the buffer wraps around, dropping the oldest historical bytes while preserving the most recent output and emitting an inline notice: `[tether: output buffer overrun; display truncated]`.

#### 5.3 Terminal Control, PTY, & Signal Management
* **FR-3.1 (Window Resizing):** Terminal dimension changes on the client (`SIGWINCH`) must be serialized into a control packet `ResizePacket(Rows, Cols)` and applied to the remote PTY master via `ioctl(TIOCSWINSZ)`.
* **FR-3.2 (Signal Isolation):** Client-side interruption signals (`Ctrl+C` / `SIGINT`) must be forwarded to the remote PTY child process without terminating the local `tether` client process.
* **FR-3.3 (Clean Disconnect):** Pressing a dedicated detach escape sequence (default: `Enter` followed by `~.` or `tether detach`) cleanly detaches the client while leaving remote processes running.

#### 5.4 Escape Sequence & Clipboard Passthrough
* **FR-4.1 (OSC 52 Passthrough):** All Operating System Command 52 (OSC 52) escape sequences (standard for terminal-to-host clipboard synchronization) must pass unmodified in both directions.
* **FR-4.2 (24-bit TrueColor & Unicode):** Full UTF-8 glyphs, emojis, and 24-bit TrueColor escape sequences must be preserved without translation or normalization.

---

### 6. Non-Functional Requirements (NFRs)

* **Performance & Latency (NFR-1):** In steady-state connected mode, `tether` shall introduce less than 1.5 milliseconds of additional latency overhead compared to raw SSH.
* **Resource Footprint (NFR-2):**
  * Memory footprint of `tetherd` on the remote server shall not exceed 25 MB RSS under full buffer allocation.
  * Idle CPU usage for both client and daemon shall remain below 0.1%.
* **Zero Dependencies (NFR-3):** Both client and daemon shall compile to statically linked binaries with zero runtime external dependencies (no shared C libraries required).
* **Security & Isolation (NFR-4):**
  * Unix domain sockets created by `tetherd` must be restricted to file permissions `0700` owned by the user.
  * Reconnection tokens must possess at least 256 bits of cryptographic entropy generated via `crypto/rand`.

---

### 7. Phased Implementation Roadmap

```
  Phase 1: Foundation (Weeks 1–3)
  ├── Basic PTY spawning & raw mode terminal proxy
  ├── Direct SSH tunneling over standard Port 22
  └── Signal forwarding (SIGWINCH, SIGINT)

  Phase 2: Persistence & Buffering (Weeks 4–6)
  ├── Ring buffer implementation with sequence tracking
  ├── Reconnection state machine with exponential backoff
  └── Output replay verification under simulated sleep/packet loss

  Phase 3: Production Hardening (Weeks 7–8)
  ├── Remote daemon auto-bootstrapping over SFTP
  ├── OSC 52 clipboard validation across terminal emulators
  └── Multi-session management & CLI polish (`tether list`, `tether attach`)
```

---

### 8. Acceptance Criteria for MVP (v0.1.0)

1. A user can run `tether user@host` and land inside their remote default login shell.
2. Launching Claude Code, executing a prompt, and closing the laptop lid for 2 minutes does not terminate the Claude process.
3. Upon opening the laptop, the session re-attaches automatically within 3 seconds, displays the newly generated tokens, and accepts keyboard input.
4. Native OS copy and paste functions normally via mouse and OSC 52 without external multiplexer tools.
