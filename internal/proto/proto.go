// Package proto implements the framed control protocol spoken between the local
// tether client and the remote tetherd daemon over an SSH channel.
//
// # Frame layout
//
//	+--------+----------------+------------------+
//	| type   | payload length | payload          |
//	| 1 byte | 4 bytes BE     | length bytes     |
//	+--------+----------------+------------------+
//
// Every frame is at most MaxFrameSize bytes of payload. The framing is
// deliberately trivial so a single ReadMessage/WriteMessage pair can move both
// terminal bytes and control messages without a stateful codec.
//
// WriteMessage emits the header and payload with exactly one call to the
// underlying writer, but it does not serialise calls: if several goroutines
// share one connection, they must hold a mutex (or funnel writes through one
// goroutine) to avoid interleaved frames.
package proto

import (
	"encoding/binary"
	"errors"
	"io"
)

// Type identifies a frame.
type Type byte

// Message types. The direction column comes from the wire specification.
const (
	TypeHello   Type = 1  // client -> server: attach request
	TypeData    Type = 2  // both: terminal bytes with their first sequence number
	TypeResize  Type = 3  // client -> server: new window size
	TypeAck     Type = 4  // client -> server: highest rendered sequence number
	TypeDetach  Type = 5  // client -> server: leave the session running
	TypeExit    Type = 6  // server -> client: child process exited
	TypePing    Type = 7  // both: heartbeat
	TypePong    Type = 8  // both: heartbeat reply
	TypeOverrun Type = 9  // server -> client: dropped byte range
	TypeError   Type = 10 // server -> client: human-readable failure
	TypeKill    Type = 11 // client -> server: terminate the session
)

// String implements fmt.Stringer for logs and errors.
func (t Type) String() string {
	switch t {
	case TypeHello:
		return "hello"
	case TypeData:
		return "data"
	case TypeResize:
		return "resize"
	case TypeAck:
		return "ack"
	case TypeDetach:
		return "detach"
	case TypeExit:
		return "exit"
	case TypePing:
		return "ping"
	case TypePong:
		return "pong"
	case TypeOverrun:
		return "overrun"
	case TypeError:
		return "error"
	case TypeKill:
		return "kill"
	default:
		return "unknown"
	}
}

const (
	// HeaderSize is the fixed frame header width: 1 byte type + 4 bytes length.
	HeaderSize = 5

	// MaxFrameSize is the largest permitted payload, 1 MiB.
	MaxFrameSize = 1 << 20

	helloSize   = 14 // version(2) + lastAckSeq(8) + rows(2) + cols(2)
	resizeSize  = 4  // rows(2) + cols(2)
	seqSize     = 8  // uint64 sequence number
	exitSize    = 4  // int32 exit code
	overrunSize = 16 // fromSeq(8) + toSeq(8)
)

// Errors returned by the codec.
var (
	// ErrFrameTooLarge is returned when a frame header announces a payload
	// larger than MaxFrameSize. It is detected before any allocation.
	ErrFrameTooLarge = errors.New("proto: frame too large")
	// ErrShortPayload is returned when a payload is too short for its type.
	ErrShortPayload = errors.New("proto: short payload")
	// ErrBadType is returned when a payload decoder receives the wrong frame type.
	ErrBadType = errors.New("proto: unexpected message type")
)

// Message is a single decoded frame.
type Message struct {
	Type    Type
	Payload []byte
}

// Hello is the first frame of every attach. LastAckSeq is the highest sequence
// number the client has already rendered, so 0 asks for the full retained
// window.
type Hello struct {
	Version    uint16
	LastAckSeq uint64
	Rows, Cols uint16
}

// Resize carries a new terminal window size in cells.
type Resize struct {
	Rows, Cols uint16
}

// ReadMessage reads one frame. It returns io.EOF when the stream ends cleanly on
// a frame boundary, and io.ErrUnexpectedEOF when it ends mid-frame.
func ReadMessage(r io.Reader) (Message, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		// io.ReadFull maps "no bytes read" to io.EOF and "partial header" to
		// io.ErrUnexpectedEOF, which is exactly the granularity we want.
		return Message{}, err
	}

	size := binary.BigEndian.Uint32(hdr[1:])
	if size > MaxFrameSize {
		return Message{}, ErrFrameTooLarge
	}

	m := Message{Type: Type(hdr[0])}
	if size == 0 {
		return m, nil
	}

	m.Payload = make([]byte, size)
	if _, err := io.ReadFull(r, m.Payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return Message{}, err
	}
	return m, nil
}

// WriteMessage writes one frame using a single Write call on w. Callers sharing
// w must serialise their calls.
func WriteMessage(w io.Writer, m Message) error {
	if len(m.Payload) > MaxFrameSize {
		return ErrFrameTooLarge
	}

	frame := make([]byte, HeaderSize+len(m.Payload))
	frame[0] = byte(m.Type)
	binary.BigEndian.PutUint32(frame[1:], uint32(len(m.Payload)))
	copy(frame[HeaderSize:], m.Payload)

	_, err := w.Write(frame)
	return err
}

// WriteData writes a TypeData frame whose payload is firstSeq followed by p.
func WriteData(w io.Writer, firstSeq uint64, p []byte) error {
	payload := make([]byte, seqSize+len(p))
	binary.BigEndian.PutUint64(payload, firstSeq)
	copy(payload[seqSize:], p)
	return WriteMessage(w, Message{Type: TypeData, Payload: payload})
}

// MarshalBinary encodes a Hello payload (14 bytes, big-endian).
func (h Hello) MarshalBinary() ([]byte, error) {
	p := make([]byte, helloSize)
	binary.BigEndian.PutUint16(p[0:2], h.Version)
	binary.BigEndian.PutUint64(p[2:10], h.LastAckSeq)
	binary.BigEndian.PutUint16(p[10:12], h.Rows)
	binary.BigEndian.PutUint16(p[12:14], h.Cols)
	return p, nil
}

// UnmarshalBinary decodes a Hello payload. It rejects any length other than 14.
func (h *Hello) UnmarshalBinary(p []byte) error {
	if len(p) != helloSize {
		return ErrShortPayload
	}
	h.Version = binary.BigEndian.Uint16(p[0:2])
	h.LastAckSeq = binary.BigEndian.Uint64(p[2:10])
	h.Rows = binary.BigEndian.Uint16(p[10:12])
	h.Cols = binary.BigEndian.Uint16(p[12:14])
	return nil
}

// MarshalBinary encodes a Resize payload (4 bytes, big-endian).
func (r Resize) MarshalBinary() ([]byte, error) {
	p := make([]byte, resizeSize)
	binary.BigEndian.PutUint16(p[0:2], r.Rows)
	binary.BigEndian.PutUint16(p[2:4], r.Cols)
	return p, nil
}

// UnmarshalBinary decodes a Resize payload. It rejects any length other than 4.
func (r *Resize) UnmarshalBinary(p []byte) error {
	if len(p) != resizeSize {
		return ErrShortPayload
	}
	r.Rows = binary.BigEndian.Uint16(p[0:2])
	r.Cols = binary.BigEndian.Uint16(p[2:4])
	return nil
}

// DataSeq decodes a TypeData payload into its first sequence number and the raw
// terminal bytes.
func DataSeq(m Message) (uint64, []byte, error) {
	if m.Type != TypeData {
		return 0, nil, ErrBadType
	}
	if len(m.Payload) < seqSize {
		return 0, nil, ErrShortPayload
	}
	return binary.BigEndian.Uint64(m.Payload), m.Payload[seqSize:], nil
}

// SeqOf decodes the sequence number carried by a TypeAck payload.
func SeqOf(m Message) (uint64, error) {
	if m.Type != TypeAck {
		return 0, ErrBadType
	}
	if len(m.Payload) != seqSize {
		return 0, ErrShortPayload
	}
	return binary.BigEndian.Uint64(m.Payload), nil
}

// ExitCode decodes the exit status carried by a TypeExit payload.
func ExitCode(m Message) (int32, error) {
	if m.Type != TypeExit {
		return 0, ErrBadType
	}
	if len(m.Payload) != exitSize {
		return 0, ErrShortPayload
	}
	return int32(binary.BigEndian.Uint32(m.Payload)), nil
}

// OverrunRange decodes the dropped half-open sequence range [from, to) carried
// by a TypeOverrun payload.
func OverrunRange(m Message) (from, to uint64, err error) {
	if m.Type != TypeOverrun {
		return 0, 0, ErrBadType
	}
	if len(m.Payload) != overrunSize {
		return 0, 0, ErrShortPayload
	}
	return binary.BigEndian.Uint64(m.Payload[0:8]), binary.BigEndian.Uint64(m.Payload[8:16]), nil
}
