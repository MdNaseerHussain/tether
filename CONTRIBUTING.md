# Contributing to tether

Thanks for taking the time to contribute. Small, focused pull requests are the
easiest to review and merge.

## Before you start

- Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): the package boundaries and the
  session/PTY lifecycle are deliberate, and changes that cross them need a
  discussion first.
- Read [docs/PROTOCOL.md](docs/PROTOCOL.md) if you are touching `internal/proto`.
  Wire changes require a `ProtocolVersion` bump and matching test vectors.
- For anything larger than a bug fix, open an issue so the design can be agreed
  before code is written.

## Development loop

```bash
make build   # bin/tether + bin/tetherd, version injected via -ldflags
make test    # go test -race ./...
make check   # gofmt check + go vet + race tests (what CI runs)
```

Requirements: Go 1.22 or newer. No cgo, no external services, no network access
in tests.

## Expectations for a pull request

- `make check` passes locally; CI runs it on Linux and macOS.
- New behaviour comes with tests. Code paths that touch the ring buffer or the
  frame codec need table-driven tests, including error cases.
- Keep the public surface of `internal/*` minimal and documented; every exported
  identifier gets a doc comment that starts with its name.
- No new dependencies without a reason in the description. The project aims to
  stay a pure-Go, statically linkable pair of binaries.
- Terminal correctness beats cleverness: bytes belong to the user's terminal, so
  do not filter, re-encode or "sanitise" them.

## Commit messages

Use the imperative mood with a conventional prefix, for example:

```
proto: reject oversized frames before allocation
ringbuf: keep ReadFrom idempotent across reconnects
client: restore the terminal when the dial fails
```

## Reporting bugs

Include the output of `tether --version`, `tetherd --version`, your local
terminal emulator, the remote OS/architecture, and a minimal reproduction. For
anything involving disconnects, say which side (client or daemon) logged the
error and whether the session survived.
