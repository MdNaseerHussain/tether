# tether

> **The persistent terminal session proxy for remote development and long-running AI streams over SSH.**

[License: MIT](LICENSE) [Go Report Card](https://goreportcard.com/report/github.com/MdNaseerHussain/tether) ![](https://img.shields.io/badge/go-1.26%2B-00ADD8)

---

## The Problem

Modern software engineering increasingly happens on remote developer instances (AWS Cloud Desktops, GCP Cloudtop, remote EC2/GCE devboxes). At the same time, terminal workflows have shifted from short, bursty shell commands to **long-running, stateful, streaming interactive AI CLI agents** (e.g., Anthropic's Claude Code, Aider, Goose) and prolonged integration test suites.

When using standard SSH:
* **Closing your laptop or switching Wi-Fi terminates the TCP socket (`broken pipe`).**
* The remote shell sends `SIGHUP` or encounters `EPIPE` on stdout, killing your active Claude Code session or losing the streamed response mid-generation.
* Existing workarounds like **Mosh** require opening high UDP ports (blocked by corporate firewalls), break native terminal scrollback, and mangle **OSC 52** clipboard synchronization.
* Workarounds like **`tmux`** hijack terminal mouse selection, split clipboard buffers, and add intrusive keybinding layers.

**`tether` solves this seamlessly.** It is a lightweight, two-binary Go client-daemon pair that runs over the SSH transport you already have: `tether` on your laptop and `tetherd` on the remote host. On first connect `tether` finds a matching `tetherd`, or uploads one to `~/.local/bin/tetherd` over SFTP without root; there is nothing to install by hand. When your connection drops, `tetherd` keeps your shell alive on the remote server, buffers output in a rolling ring buffer, and instantly re-attaches and replays missed tokens the second your laptop wakes up.

---

## Comparison: `tether` vs. Existing Tools

| Capability / Attribute | Standard SSH | Mosh | Eternal Terminal (ET) | `tmux` over SSH | `tether` |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Survives Laptop Sleep & Wi-Fi Handoffs** | ❌ No | ✅ Yes | ✅ Yes | ⚠️ Manual Re-attach | ✅ **Seamless Auto-Resume** |
| **Preserves Long-Running AI/CLI Streams** | ❌ Drops stream | ⚠️ Re-renders screen | ⚠️ Buffer replay | ⚠️ Manual reconnect | ✅ **In-Memory Ring Buffer Replay** |
| **Network & Firewall Requirements** | Standard Port 22 | High UDP (60000–61000) | Custom TCP (Port 2022) | Standard Port 22 | **Standard Port 22 (SSH Exec Channel)** |
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

1. **Local Client (`tether`):** Switches your local terminal into raw mode and captures signals (like `SIGWINCH` for terminal resizing). It authenticates over standard SSH using `ssh-agent`, an identity file (`-i`), the default keys under `~/.ssh`, or an interactive password prompt, then runs `tetherd attach` on the remote host.
2. **Server Daemon (`tetherd`):** Started on the remote server by the client with `tetherd attach --spawn`, or explicitly with `tetherd serve`. If no matching binary is on the remote host, the client probes `$PATH` and `~/.local/bin`, then uploads a matching `tetherd` (mode 0755) over SFTP. It creates a Unix Pseudo-Terminal (PTY) and spawns your shell in its own process session (`setsid`), so it outlives the SSH channel.
3. **Rolling Stream Buffer:** Terminal output is continuously pushed into an in-memory rolling ring buffer (default: 4 MB). If the client disconnects, stdout writes do not block or fail.
4. **Resumption Handshake:** When your laptop wakes up, `tether` re-dials SSH and re-authenticates with your agent/keys (there is no session token to store), attaches to the same session id, synchronizes terminal window dimensions, and replays unacknowledged bytes from the last acknowledged sequence number.

---

## Quickstart

### 1. Installation

#### Homebrew (macOS & Linux)
```bash
brew install MdNaseerHussain/tap/tether
```

#### Go Install
```bash
go install github.com/MdNaseerHussain/tether/cmd/tether@latest
```

#### Pre-built Binaries
Download the latest static binary for macOS (`arm64`/`amd64`) or Linux (`amd64`/`arm64`) from the [Releases](https://github.com/MdNaseerHussain/tether/releases) page.

### 2. Usage

`tether` takes the same `[user@]host` shape as `ssh` — an `~/.ssh/config` alias
or a literal host — plus its own flags:

```bash
# Attach to (or create) the "default" session using an ssh_config alias.
# tetherd is uploaded to ~/.local/bin/tetherd automatically if it is missing
# or does not match this client's version.
tether cloudtop

# Custom key + port, and a named session.
tether -i ~/.ssh/id_ed25519 -p 2222 --session dev developer@devbox.internal

# Keep an explicitly installed remote binary, or skip bootstrapping entirely.
tether --remote-bin /opt/bin/tetherd host
tether --no-bootstrap host

# Point at a non-default ssh_config, or ignore it completely.
tether --ssh-config ~/.ssh/config.work work-host
tether --no-ssh-config 10.0.0.5

# Custom detach sequence, and a larger buffer for a newly created session.
tether -l deploy --escape '~^' --buffer-size 8388608 host.example.com
```

Host, user, port, identity files, `IdentityAgent` and `ProxyCommand` are
resolved from `~/.ssh/config` the way `ssh` resolves them (first value wins;
`IdentityFile` accumulates). Explicit flags win over the config, which wins over
the built-in defaults; `user@alias` overrides the config user. Authentication
still goes agent → identity files → interactive password on `/dev/tty`, and
`ProxyCommand` runs through `/bin/sh` with `%h`, `%n`, `%p`, `%r` substituted.
Pass `--insecure` only to skip host-key verification for a throwaway host.

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
* **Invisible UX:** No status bars, no screen splitting, no custom escape keybindings (`Ctrl+B`). Your terminal behaves exactly like a native terminal (`~.` detaches, configurable with `--escape`).
* **Key- or agent-based reconnect:** A dropped channel is re-dialled with your ssh-agent or on-disk keys; no separate session token is issued or stored.
* **Works like `ssh` for setup:** `tether <alias>` honours `HostName`, `User`, `Port`, `IdentityFile`, `IdentityAgent` and `ProxyCommand` from `~/.ssh/config`; explicit flags override them.
* **Self-installing remote daemon:** If `tetherd` is missing or mismatched, the client uploads a matching static binary to `~/.local/bin/tetherd` over the authenticated SSH connection. No root, no package manager, no manual copy; `--no-bootstrap` opts out.


---

## Roadmap

- [x] Initial architecture specification and PTY session lifecycle design
- [x] Core PTY allocation and raw-mode client engine in Go
- [x] Ring buffer stream capture and sequence replay
- [x] Reconnection state machine with exponential backoff
- [x] Automated remote bootstrap over standard SSH (SFTP upload to `~/.local/bin/tetherd`)
- [x] `~/.ssh/config` host, user, port, identity-agent and `ProxyCommand` resolution
- [ ] Session detachment and multi-device attach support (detach is implemented;
      the daemon accepts concurrent attaches, but multi-device use is not advertised yet)

Limitations are deliberate: there is no Windows client, session state is
in-memory only (a daemon crash loses the ring buffer), and reconnect
re-authenticates with ssh-agent or key files — tether stores no session token.

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

`make check` additionally verifies formatting with `gofmt` and runs `go vet`, which is what CI enforces. `make cross` produces static (`CGO_ENABLED=0`) release binaries for Linux and macOS on `amd64` and `arm64`, while `make dist` writes the flat bootstrap assets `dist/tetherd-<os>-<arch>` (and the matching client) that the uploader looks for. `make install` puts both commands in `$(go env GOPATH)/bin`.

The client uploads `tetherd` when the remote host has none or has a different version: it probes `tetherd` on `$PATH`, then `$HOME/.local/bin/tetherd`, derives the platform with `uname -s; uname -m`, and stages a matching asset over SFTP with a temporary name plus an atomic rename. It launches the result with `tetherd attach --spawn` over an SSH exec channel; the daemon keeps the session alive across disconnects. Reconnect re-authenticates with your ssh-agent or key files and reuses the resolved binary path; no session token is stored. Pass `--no-bootstrap`, `--remote-bin <path>`, or `--assets-dir <dir>` to control it.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the component design and [docs/PROTOCOL.md](docs/PROTOCOL.md) for the byte-level wire format.
