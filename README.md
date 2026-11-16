# tether

> **The persistent, zero-config terminal session proxy for remote development and long-running AI streams over SSH.**

[License: MIT](LICENSE) [Go Report Card](https://goreportcard.com/report/github.com/naseer-hussain/tether) ![](https://img.shields.io/badge/go-1.22%2B-00ADD8)

---

## The Problem

Modern software engineering increasingly happens on remote developer instances (AWS Cloud Desktops, GCP Cloudtop, remote EC2/GCE devboxes). At the same time, terminal workflows have shifted from short, bursty shell commands to **long-running, stateful, streaming interactive AI CLI agents** (e.g., Anthropic's Claude Code, Aider, Goose) and prolonged integration test suites.

When using standard SSH:
* **Closing your laptop or switching Wi-Fi terminates the TCP socket (`broken pipe`).**
* The remote shell sends `SIGHUP` or encounters `EPIPE` on stdout, killing your active Claude Code session or losing the streamed response mid-generation.
* Existing workarounds like **Mosh** require opening high UDP ports (blocked by corporate firewalls), break native terminal scrollback, and mangle **OSC 52** clipboard synchronization.
* Workarounds like **`tmux`** hijack terminal mouse selection, split clipboard buffers, and add intrusive keybinding layers.

**`tether` solves this seamlessly.** It is a lightweight, single-binary Go client-daemon pair that tunnels over your existing SSH configuration. When your connection drops, `tetherd` keeps your shell alive on the remote server, buffers output in a rolling ring buffer, and instantly re-attaches and replays missed tokens the second your laptop wakes up.

---

## Comparison: `tether` vs. Existing Tools

| Capability / Attribute | Standard SSH | Mosh | Eternal Terminal (ET) | `tmux` over SSH | `tether` |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Survives Laptop Sleep & Wi-Fi Handoffs** | ❌ No | ✅ Yes | ✅ Yes | ⚠️ Manual Re-attach | ✅ **Seamless Auto-Resume** |
| **Preserves Long-Running AI/CLI Streams** | ❌ Drops stream | ⚠️ Re-renders screen | ⚠️ Buffer replay | ⚠️ Manual reconnect | ✅ **In-Memory Ring Buffer Replay** |
| **Network & Firewall Requirements** | Standard Port 22 | High UDP (60000–61000) | Custom TCP (Port 2022) | Standard Port 22 | **Standard Port 22 (SSH Subsystem)** |
| **Native OS Copy & Paste (OSC 52)** | ✅ Native | ❌ Stripped/Broken | ⚠️ Partial | ❌ Hijacked / Broken | ✅ **Zero-Interference Passthrough** |
| **Native Terminal Scrollback** | ✅ Yes | ❌ Requires screen hack | ✅ Yes | ❌ Requires copy-mode | ✅ **Native Local Terminal Buffer** |
| **Root / Sudo Privileges Required** | ❌ No | ❌ No | ⚠️ Yes (Daemon install) | ❌ No | ❌ **Zero Root / Pure User Space** |
| **Keybinding Collisions** | None | None | None | High (`Ctrl+B`, mouse traps) | **None (Invisible Proxy)** |
| **Client Terminal Flexibility** | Any | Any | Any | Any | **Any (iTerm2, Alacritty, Kitty, Ghostty)** |
| **Binary Footprint & Dependencies** | Native | C++ / Protobuf | C++ / Boost / Protobuf | C / libevent | **Pure Go (<15 MB Static Binary)** |

> For an in-depth architectural and protocol comparison, see [docs/COMPARISON.md](docs/COMPARISON.md).

---

## Architecture Overview

`tether` operates as a transparent layer between your local terminal emulator and your remote shell:

```
[Local Machine]                                          [Remote Server / Cloudtop]
┌────────────────────────┐                                ┌────────────────────────┐
│ Terminal (Alacritty,   │                                │ Remote Host            │
│ iTerm2, Kitty, etc.)   │                                │                        │
│           │            │                                │                        │
│     (Raw Mode PTY)     │                                │                        │
│           ▼            │                                │                        │
│      tether client     │   Encrypted SSH / TCP Channel  │     tetherd daemon     │
│  ┌──────────────────┐  │        (Port 22 Tunnel)        │  ┌──────────────────┐  │
│  │ Reconnect Loop   │  │ ══════════════════════════════ │  │ Session Manager  │  │
│  │ Signal Router    │  │ <═════════════════════════════ │  │ Rolling Buffer   │  │
│  │ Heartbeat Monitor│  │                                │  │ PTY Controller   │  │
│  └──────────────────┘  │                                │  └────────┬─────────┘  │
└────────────────────────┘                                └───────────┼────────────┘
                                                                       ▼
                                                             ┌──────────────────┐
                                                             │ Child Shell /    │
                                                             │ Claude CLI /     │
                                                             │ Long Build Job   │
                                                             └──────────────────┘
```

1. **Local Client (`tether`):** Switches your local terminal into raw mode and captures signals (like `SIGWINCH` for terminal resizing). It connects to the remote machine via standard SSH.
2. **Server Daemon (`tetherd`):** Launched automatically in the background on the remote server (or bootstrapped via SSH). It creates a Unix Pseudo-Terminal (PTY) and spawns your default shell.
3. **Rolling Stream Buffer:** Terminal output is continuously pushed into an in-memory rolling ring buffer (default: 4 MB). If the client disconnects, stdout writes do not block or fail.
4. **Resumption Handshake:** When your laptop wakes up, `tether` reconnects to the existing session token, synchronizes terminal window dimensions, and replays unacknowledged bytes.

---

## Quickstart

### 1. Installation

#### Homebrew (macOS & Linux)
```bash
brew install naseer-hussain/tap/tether
```

#### Go Install
```bash
go install github.com/naseer-hussain/tether/cmd/tether@latest
```

#### Pre-built Binaries
Download the latest static binary for macOS (`arm64`/`amd64`) or Linux (`amd64`/`arm64`) from the [Releases](https://github.com/naseer-hussain/tether/releases) page.

### 2. Usage

Connecting with `tether` uses the exact same syntax as your regular `ssh` command:

```bash
# Connect to your remote host (auto-bootstraps tetherd if needed)
tether user@cloudtop.corp.internal

# Connect using custom SSH keys or ports
tether -i ~/.ssh/id_ed25519 -p 2222 developer@devbox.internal
```

### 3. Verify Resilience

1. Start an interactive Claude Code turn or long-running command on your remote server:
   ```bash
   claude
   # Or run a test stream:
   for i in {1..100}; do echo "Streaming step $i..."; sleep 1; done
   ```
2. Close your laptop lid or disconnect Wi-Fi for 30 seconds.
3. Re-open your laptop.
4. **Notice:** `tether` re-attaches instantly, replays all streamed steps generated while disconnected, and keeps your interactive prompt ready for input.

---

## Key Features

* **Zero-Firewall Friction:** Works entirely through standard SSH transport (port 22). No open UDP ports or firewall tickets required.
* **Stream-Safe Ring Buffer:** Maintains an in-memory rolling buffer so active AI agents never crash from broken pipes while you are offline.
* **Native Clipboard (OSC 52):** Uninterrupted bi-directional clipboard sync. Copying text in Vim or terminal tools uses your local system clipboard out of the box.
* **Invisible UX:** No status bars, no screen splitting, no custom escape keybindings (`Ctrl+B`). Your terminal behaves exactly like a native terminal.
* **Self-Bootstrapping Daemon:** If `tetherd` is not installed on the remote machine, the client can auto-stage a standalone static binary over SFTP/SCP into `~/.local/bin/`.

---

## Roadmap

- [x] Initial architecture specification and PTY session lifecycle design
- [ ] Core PTY allocation and raw-mode client engine in Go
- [ ] Ring buffer stream capture and sequence replay
- [ ] Reconnection state machine with exponential backoff
- [ ] Automated remote bootstrap over standard SSH
- [ ] Session detachment and multi-device attach support

---

## Contributing

Contributions, bug reports, and suggestions are welcome! Please check out [CONTRIBUTING.md](CONTRIBUTING.md) and review the [Product Requirements Document (PRD)](docs/PRD.md) before submitting Pull Requests.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Development

Build both binaries into `bin/` (the version string is injected via `-ldflags`):

```bash
make build
```

Run the race-enabled test suite:

```bash
make test
```

`make check` additionally verifies formatting with `gofmt` and runs `go vet`, which is what CI enforces. `make cross` produces static (`CGO_ENABLED=0`) release binaries for Linux and macOS on `amd64` and `arm64`, and `make install` puts them in `$(go env GOPATH)/bin`.

Note that `tether` alone is not enough: **`tetherd` must be installed on the remote host** you want to connect to (or reachable on its `PATH`, for example in `~/.local/bin`). The client launches it through an SSH subsystem channel and it is the daemon that keeps the session alive across disconnects.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the component design and [docs/PROTOCOL.md](docs/PROTOCOL.md) for the byte-level wire format.
