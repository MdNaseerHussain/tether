# tether wire protocol — version 1

Normative reference for `internal/proto`. The Go definitions are the
implementation; this document is the specification and the two must change
together.

## 1. Transport

The protocol runs over a single bidirectional byte stream: the SSH channel
created by requesting the `tether` subsystem (`ssh -s host tether`) or by running
`tetherd attach <id>` over an exec channel as a fallback.

Properties the protocol relies on and does not provide itself:

- Ordered, reliable, byte-exact delivery. SSH does this; the protocol has no
  sequence numbers of its own at the transport layer.
- No framing from SSH: the channel is a stream, so every message must be framed
  (below).
- The connection carries both directions. There is no separate channel for
  control and data.

Nothing is negotiated before the first frame: the client may write immediately,
and the `Hello` frame is always the client's first frame.

## 2. Frame layout

Every frame is a 5-byte header followed by a payload of the announced length.

```
 byte offset   0        1                  2                    5
            +--------+------------------+---------- - - - ----+----------------+
            | type   | payload length   | payload             |                |
            | uint8  | uint32, big end. | length bytes        |                |
            +--------+------------------+---------------------+----------------+
             1 byte        4 bytes            N bytes
```

- `type` — one of the values in §3. Unknown types are read and discarded by the
  receiver's dispatcher; they are not a decode error, which keeps the protocol
  forward-extensible in the "ignore what you do not know" direction.
- `payload length` — unsigned 32-bit big-endian payload size in bytes. `0` is
  legal and used by `Ping`, `Pong`, `Detach` and `Kill`.
- Total frame size on the wire is `5 + payloadLength`.

### 2.1 Limits

| Name | Value | Meaning |
| :-- | :-- | :-- |
| `HeaderSize` | 5 | fixed header width |
| `MaxFrameSize` | `1 << 20` (1 048 576) | maximum payload length |

A header announcing `payloadLength > MaxFrameSize` is rejected **before any
allocation** with `ErrFrameTooLarge`; the receiver must close the connection
after this error, because the stream position is unrecoverable (the sender wrote
bytes the receiver refused to consume).

### 2.2 Write discipline

`WriteMessage` builds the header and payload into one buffer and issues exactly
**one** `Write` call. It does **not** serialise concurrent callers: two
goroutines writing to the same `io.Writer` can interleave frames. Every connection
must have a single writer goroutine, or a mutex around all writes.

### 2.3 Read semantics

`ReadMessage` returns:

| Situation | Error |
| :-- | :-- |
| EOF before any byte of a new header | `io.EOF` (clean close) |
| EOF after a partial header (1–4 bytes) | `io.ErrUnexpectedEOF` |
| EOF inside a payload | `io.ErrUnexpectedEOF` |
| `payloadLength > MaxFrameSize` | `ErrFrameTooLarge` (nothing allocated) |
| otherwise | `nil`, with `Message{Type, Payload}` |

A zero-length payload is returned as a zero-length slice.

## 3. Message types

| Value | Name | Direction | Payload |
| ---: | :-- | :-- | :-- |
| 1 | `Hello` | client → server | 14 bytes, §3.1 |
| 2 | `Data` | both | 8 + N bytes, §3.2 |
| 3 | `Resize` | client → server | 4 bytes, §3.3 |
| 4 | `Ack` | client → server | 8 bytes, §3.4 |
| 5 | `Detach` | client → server | 0 bytes |
| 6 | `Exit` | server → client | 4 bytes, §3.5 |
| 7 | `Ping` | both | 0 bytes |
| 8 | `Pong` | both | 0 bytes |
| 9 | `Overrun` | server → client | 16 bytes, §3.6 |
| 10 | `Error` | server → client | UTF-8 text, §3.7 |
| 11 | `Kill` | client → server | 0 bytes |

All integers are **big-endian** (`encoding/binary`, no varints, no JSON, no
length-prefixed strings).

### 3.1 Hello (type 1) — 14 bytes

Sent once per attach, first frame from the client.

```
 0                   1                   2
 +---------+-------------------------------------------+---------+---------+
 | version |              lastAckSeq                    |  rows   |  cols   |
 | uint16  |                 uint64                    | uint16  | uint16  |
 +---------+-------------------------------------------+---------+---------+
   2 bytes                 8 bytes                    2 bytes   2 bytes
```

| Field | Offset | Width | Meaning |
| :-- | ---: | ---: | :-- |
| `version` | 0 | 2 | protocol version, currently `1` |
| `lastAckSeq` | 2 | 8 | highest sequence number already rendered; `0` = send everything retained |
| `rows` | 10 | 2 | terminal rows in cells |
| `cols` | 12 | 2 | terminal columns in cells |

Version handling: a server whose `ProtocolVersion` differs closes the channel and
sends `TypeError` with a message naming both versions when the client's version is
at least `1`; a client whose `Hello` gets no `Data`/`Exit`/`Error` within 10 s
reports "protocol mismatch or unresponsive daemon".

### 3.2 Data (type 2)

```
 0                                    8
 +------------------------------------+--------------------------- - - +
 |             firstSeq               |  raw terminal bytes           |
 |               uint64               |  N bytes, uninterpreted       |
 +------------------------------------+--------------------------- - - +
```

- `firstSeq` — sequence number of the first byte in the payload. The `n`-th byte
  of the payload has sequence `firstSeq + n`; bytes are 1-based over the whole
  session.
- The byte payload is opaque. It can contain NUL, `0x03`, split UTF-8 sequences,
  CSI/OSC/DCS escapes, mouse reports, bracketed-paste markers. The receiver copies
  it to the terminal with no inspection or translation.
- Empty data payloads (exactly 8 bytes) are legal and carry no bytes; senders
  should avoid them.
- Maximum N is `MaxFrameSize - 8` = 1 048 568 bytes, because the payload includes
  `firstSeq`.

In both directions: client → server `Data` is keyboard/paste input (the daemon
writes it to the PTY master); server → client `Data` is PTY output (the client
writes it to its tty).

### 3.3 Resize (type 3) — 4 bytes

```
 0                   2                   4
 +-------------------+-------------------+
 |       rows        |       cols        |
 |      uint16       |      uint16       |
 +-------------------+-------------------+
```

Sent whenever the local terminal changes size (`SIGWINCH`), and again after
reconnecting only if the size differs from the last one sent.

### 3.4 Ack (type 4) — 8 bytes

```
 +------------------------------------+
 |               seq                  |
 |              uint64                |
 +------------------------------------+
```

`seq` is the highest sequence number the client has written to its terminal and
flushed. It is cumulative and only ever increases within a session. Servers treat
a decreasing `seq` as a client bug and clamp it to the highest value seen.

The server does not use `Ack` to trim the ring buffer (retention is fixed); it
exists solely to let a reconnecting client resume exactly.

### 3.5 Exit (type 6) — 4 bytes

```
 +------------------------------------+
 |               code                 |
 |              int32                 |
 +------------------------------------+
```

The child process's exit status (`0`–`255` for normal exits, `-1`/`128+signal`
conventions per Go's `os.ProcessState.ExitCode`). After sending `Exit` the server
closes the channel.

### 3.6 Overrun (type 9) — 16 bytes

```
 0                                   8                                  16
 +-----------------------------------+-----------------------------------+
 |              fromSeq              |               toSeq               |
 |              uint64               |               uint64              |
 +-----------------------------------+-----------------------------------+
```

Reports that bytes with sequence numbers in the **half-open** range
`[fromSeq, toSeq)` were dropped and can never be replayed. `fromSeq` is
`lastAckSeq + 1` from the hello, and `toSeq` is the sequence of the first byte of
the `Data` frame that follows, so the two are contiguous.

On receipt the client prints `ringbuf.NoticeOverrun` (or a terminal-appropriate
rendering of it) once, then renders the replay. If `toSeq <= fromSeq` the range is
empty and the frame is ignored.

### 3.7 Error (type 10)

Payload is UTF-8 text with no internal length prefix — the frame length delimits
it. It is a human-readable diagnostic ("no such session: abc12345", "session
busy", "protocol version 3 not supported"), never machine-parsed. The client
prints it to stderr (outside raw mode if possible), and the server closes the
channel after sending it.

Maximum length `MaxFrameSize`; senders should keep it under 4 KiB.

## 4. Session flows

### 4.1 Fresh attach

```
client                                          server
  │ ── Hello{1, 0, 40, 120} ───────────────────>│
  │                                             │  ring.ReadFrom(0)
  │ <──────────────────── Data{firstSeq=1, ...} │
  │ <──────────────────── Data{firstSeq=…, ...} │
  │ <────────────────────────────────────────── │ (PTY output continues)
  │ ── Ack{seq} ───────────────────────────────>│  (≤ every 100 ms)
  │ ── Ping ───────────────────────────────────>│
  │ <──────────────────── Pong ─────────────────│
  │ ── Resize{50, 200} ────────────────────────>│
  │ ── Data{firstSeq=0, "\x03"} ───────────────>│  (ctrl-c as a byte)
  │ <──────────────────── Data{firstSeq=…,…} ───│
  │ <──────────────────── Exit{0} ──────────────│
```

`Data` frames sent client → server carry `firstSeq = 0`: input bytes have no
sequence space of their own (they are not replayed). A server must ignore the
field on inbound `Data`.

### 4.2 Reconnect after an outage

```
client                                          server
  │  (socket died, 47 s of output produced)      │
  │ ── Hello{1, 12345, 40, 120} ───────────────>│
  │                                             │  ReadFrom(12345)
  │ <──────────────────── Overrun{12346, 90000} │  (only if 12346 < oldest)
  │ <──────────────────── Data{firstSeq=90000, …}│
  │ ── Ack{…} ─────────────────────────────────>│
```

If the ack still falls inside the window, no `Overrun` is sent and the replay
starts exactly after `lastAckSeq`.

### 4.3 Detach

```
  │ ── Detach ─────────────────────────────────>│
  │ <──────────────────── (channel closed) ─────│  session keeps running
```

The client then restores its terminal and exits 0. If the channel is not closed
within 500 ms the client closes it itself.

### 4.4 Kill

```
  │ ── Kill ───────────────────────────────────>│  SIGTERM → pgrp, 5 s, SIGKILL
  │ <──────────────────── Exit{143} ────────────│
```

`Kill` is only accepted while attached or immediately after `Hello`; the session
is removed after the exit frame is delivered (or after 2 s if nobody is there to
receive it).

## 5. Worked examples (hex)

`Ping` — five bytes, no payload:

```
07 00 00 00 00
```

`Hello{version:1, lastAckSeq:0, rows:24, cols:80}`:

```
01 00 00 00 0E              frame: type 1, length 14
00 01                       version 1
00 00 00 00 00 00 00 00     lastAckSeq 0
00 18                       rows 24
00 50                       cols 80
```

`Data` carrying `hi` at sequence 1 (payload length 10):

```
02 00 00 00 0A              frame: type 2, length 10
00 00 00 00 00 00 00 01     firstSeq 1
68 69                       "hi"
```

`Ack{57}`:

```
04 00 00 00 08  00 00 00 00 00 00 00 39
```

`Overrun{fromSeq:100, toSeq:4096}`:

```
09 00 00 00 10
00 00 00 00 00 00 00 64
00 00 00 00 00 00 10 00
```

`Exit{code:130}`:

```
06 00 00 00 04  00 00 00 82
```

`Error("session busy")`:

```
0A 00 00 00 0C  73 65 73 73 69 6F 6E 20 62 75 73 79
```

## 6. Go API surface

```go
ReadMessage(r io.Reader) (Message, error)
WriteMessage(w io.Writer, m Message) error
WriteData(w io.Writer, firstSeq uint64, p []byte) error

Hello{Version uint16; LastAckSeq uint64; Rows, Cols uint16}
(h Hello) MarshalBinary() ([]byte, error)       // always 14 bytes
(h *Hello) UnmarshalBinary(p []byte) error      // ErrShortPayload unless len == 14

Resize{Rows, Cols uint16}
(r Resize) MarshalBinary() ([]byte, error)      // always 4 bytes
(r *Resize) UnmarshalBinary(p []byte) error     // ErrShortPayload unless len == 4

DataSeq(m Message) (seq uint64, p []byte, err error)   // TypeData only
SeqOf(m Message) (seq uint64, err error)               // TypeAck only
ExitCode(m Message) (code int32, err error)            // TypeExit only
OverrunRange(m Message) (from, to uint64, err error)   // TypeOverrun only
```

Sentinel errors: `ErrFrameTooLarge`, `ErrShortPayload`, `ErrBadType`.

`DataSeq` returns a view into the frame's payload slice; the caller owns the
message and must not reuse the buffer while holding the view.

## 7. Compatibility rules

1. Adding a new message type is backward compatible: peers ignore unknown types.
2. Changing a payload layout requires a `ProtocolVersion` bump. A peer must never
   interpret a payload of the wrong width — `UnmarshalBinary` returns
   `ErrShortPayload` for any unexpected length.
3. `Hello.Version` is the only negotiation. There is no downgrade path; both
   binaries ship together through `tether`'s bootstrap (§11 of the architecture
   doc).
4. Reserved for later: `TypeHello` environment/tags fields, `Data` compression
   flag bits, `Ack` cumulative drop counters. None exist in version 1; the frame
   length is what future readers use to detect them.
