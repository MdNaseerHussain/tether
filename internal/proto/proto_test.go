package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestRoundTripAllTypes(t *testing.T) {
	helloPayload, err := Hello{Version: 1, LastAckSeq: 1 << 40, Rows: 50, Cols: 200}.MarshalBinary()
	if err != nil {
		t.Fatalf("Hello.MarshalBinary: %v", err)
	}
	resizePayload, err := Resize{Rows: 24, Cols: 80}.MarshalBinary()
	if err != nil {
		t.Fatalf("Resize.MarshalBinary: %v", err)
	}
	ackPayload := make([]byte, 8)
	binary.BigEndian.PutUint64(ackPayload, 123456789)
	exitPayload := make([]byte, 4)
	binary.BigEndian.PutUint32(exitPayload, uint32(3))
	overrunPayload := make([]byte, 16)
	binary.BigEndian.PutUint64(overrunPayload[0:8], 100)
	binary.BigEndian.PutUint64(overrunPayload[8:16], 4096)

	cases := []Message{
		{Type: TypeHello, Payload: helloPayload},
		{Type: TypeData, Payload: append([]byte{0, 0, 0, 0, 0, 0, 0, 9}, []byte("terminal bytes\x1b[31m")...)},
		{Type: TypeResize, Payload: resizePayload},
		{Type: TypeAck, Payload: ackPayload},
		{Type: TypeDetach},
		{Type: TypeExit, Payload: exitPayload},
		{Type: TypePing},
		{Type: TypePong},
		{Type: TypeOverrun, Payload: overrunPayload},
		{Type: TypeError, Payload: []byte("no such session")},
		{Type: TypeKill},
	}

	for _, want := range cases {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, want); err != nil {
			t.Fatalf("%s: WriteMessage: %v", want.Type, err)
		}
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("%s: ReadMessage: %v", want.Type, err)
		}
		if got.Type != want.Type {
			t.Fatalf("%s: type = %d, want %d", want.Type, got.Type, want.Type)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("%s: payload = %q, want %q", want.Type, got.Payload, want.Payload)
		}
	}
}

func TestFrameLayoutIsByteExact(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMessage(&buf, Message{Type: TypeData, Payload: []byte{0xAA, 0xBB}})
	if err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	want := []byte{0x02, 0x00, 0x00, 0x00, 0x02, 0xAA, 0xBB}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("frame = % x, want % x", buf.Bytes(), want)
	}

	// A frame is exactly one Write call.
	w := &writeCounter{}
	if err := WriteMessage(w, Message{Type: TypePing}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	if w.calls != 1 {
		t.Fatalf("Write calls = %d, want 1", w.calls)
	}
	if !bytes.Equal(w.data, []byte{0x07, 0x00, 0x00, 0x00, 0x00}) {
		t.Fatalf("ping frame = % x", w.data)
	}
}

func TestMultiFrameStream(t *testing.T) {
	var stream bytes.Buffer
	hello := Hello{Version: 1, LastAckSeq: 42, Rows: 30, Cols: 120}
	helloPayload, _ := hello.MarshalBinary()

	if err := WriteMessage(&stream, Message{Type: TypeHello, Payload: helloPayload}); err != nil {
		t.Fatal(err)
	}
	if err := WriteData(&stream, 43, []byte("$ echo hi\r\nhi\r\n")); err != nil {
		t.Fatal(err)
	}
	ackPayload := make([]byte, 8)
	binary.BigEndian.PutUint64(ackPayload, 57)
	if err := WriteMessage(&stream, Message{Type: TypeAck, Payload: ackPayload}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(&stream, Message{Type: TypePing}); err != nil {
		t.Fatal(err)
	}

	msgs := make([]Message, 0, 4)
	for {
		m, err := ReadMessage(&stream)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		msgs = append(msgs, m)
	}
	if len(msgs) != 4 {
		t.Fatalf("read %d frames, want 4", len(msgs))
	}

	var gotHello Hello
	if err := gotHello.UnmarshalBinary(msgs[0].Payload); err != nil {
		t.Fatalf("Hello.UnmarshalBinary: %v", err)
	}
	if gotHello != hello {
		t.Fatalf("hello = %+v, want %+v", gotHello, hello)
	}

	seq, data, err := DataSeq(msgs[1])
	if err != nil {
		t.Fatalf("DataSeq: %v", err)
	}
	if seq != 43 || string(data) != "$ echo hi\r\nhi\r\n" {
		t.Fatalf("data frame = (%d, %q)", seq, data)
	}

	ack, err := SeqOf(msgs[2])
	if err != nil {
		t.Fatalf("SeqOf: %v", err)
	}
	if ack != 57 {
		t.Fatalf("ack = %d, want 57", ack)
	}
	if msgs[3].Type != TypePing {
		t.Fatalf("last frame = %s, want ping", msgs[3].Type)
	}
}

func TestReadMessageCleanEOF(t *testing.T) {
	if _, err := ReadMessage(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}

	// EOF on a frame boundary after a complete frame is a clean EOF too.
	var buf bytes.Buffer
	WriteMessage(&buf, Message{Type: TypePong})
	if _, err := ReadMessage(&buf); err != nil {
		t.Fatalf("first frame: %v", err)
	}
	if _, err := ReadMessage(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestReadMessageTruncated(t *testing.T) {
	var full bytes.Buffer
	if err := WriteData(&full, 7, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	frame := full.Bytes()

	// Truncated header.
	if _, err := ReadMessage(bytes.NewReader(frame[:3])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated header: err = %v, want io.ErrUnexpectedEOF", err)
	}

	// Truncated payload.
	if _, err := ReadMessage(bytes.NewReader(frame[:len(frame)-4])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated payload: err = %v, want io.ErrUnexpectedEOF", err)
	}

	// Header only, payload missing entirely.
	if _, err := ReadMessage(bytes.NewReader(frame[:HeaderSize])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("missing payload: err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// headerOnlyReader serves exactly one header and fails if asked for more, so a
// decoder that allocates or reads the payload before validating the length is
// caught.
type headerOnlyReader struct {
	hdr  []byte
	read bool
}

func (r *headerOnlyReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, errors.New("decoder read the payload of an oversized frame")
	}
	r.read = true
	return copy(p, r.hdr), nil
}

func TestReadMessageRejectsOversizedFrame(t *testing.T) {
	hdr := make([]byte, HeaderSize)
	hdr[0] = byte(TypeData)
	binary.BigEndian.PutUint32(hdr[1:], MaxFrameSize+1)

	_, err := ReadMessage(&headerOnlyReader{hdr: hdr})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}

	// The bound itself is legal.
	hdr2 := make([]byte, HeaderSize)
	hdr2[0] = byte(TypeData)
	binary.BigEndian.PutUint32(hdr2[1:], MaxFrameSize)
	if _, err := ReadMessage(&headerOnlyReader{hdr: hdr2}); errors.Is(err, ErrFrameTooLarge) {
		t.Fatal("MaxFrameSize payload must be accepted by the length check")
	}
}

func TestWriteMessageRejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	err := WriteMessage(&buf, Message{Type: TypeData, Payload: make([]byte, MaxFrameSize+1)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("wrote %d bytes for oversized frame, want 0", buf.Len())
	}

	if err := WriteData(&buf, 1, make([]byte, MaxFrameSize)); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("WriteData with oversized payload: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestHelloMarshalBinary(t *testing.T) {
	h := Hello{Version: 1, LastAckSeq: 0x0102030405060708, Rows: 0x0A0B, Cols: 0x0C0D}
	got, err := h.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	want := []byte{
		0x00, 0x01, // version 1
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // lastAckSeq
		0x0A, 0x0B, // rows
		0x0C, 0x0D, // cols
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("hello = % x, want % x", got, want)
	}
	if len(got) != 14 {
		t.Fatalf("len = %d, want 14", len(got))
	}

	var back Hello
	if err := back.UnmarshalBinary(got); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if back != h {
		t.Fatalf("round trip = %+v, want %+v", back, h)
	}
}

func TestHelloUnmarshalRejectsBadLength(t *testing.T) {
	for _, n := range []int{0, 1, 13, 15, 20} {
		var h Hello
		if err := h.UnmarshalBinary(make([]byte, n)); err == nil {
			t.Fatalf("len %d: expected error, got nil", n)
		}
	}
	// The decoder must not have partially clobbered the receiver.
	h := Hello{Version: 2, LastAckSeq: 9, Rows: 1, Cols: 2}
	if err := h.UnmarshalBinary(make([]byte, 4)); err == nil {
		t.Fatal("expected error")
	}
	if h != (Hello{Version: 2, LastAckSeq: 9, Rows: 1, Cols: 2}) {
		t.Fatalf("receiver mutated on error: %+v", h)
	}
}

func TestResizeMarshalBinary(t *testing.T) {
	r := Resize{Rows: 0x0102, Cols: 0x0304}
	got, err := r.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	if !bytes.Equal(got, []byte{0x01, 0x02, 0x03, 0x04}) {
		t.Fatalf("resize = % x, want 01 02 03 04", got)
	}

	var back Resize
	if err := back.UnmarshalBinary(got); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if back != r {
		t.Fatalf("round trip = %+v, want %+v", back, r)
	}
}

func TestResizeUnmarshalRejectsBadLength(t *testing.T) {
	for _, n := range []int{0, 3, 5, 8} {
		var r Resize
		if err := r.UnmarshalBinary(make([]byte, n)); err == nil {
			t.Fatalf("len %d: expected error, got nil", n)
		}
	}
}

func TestDataSeq(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteData(&buf, 1<<33, []byte("payload")); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	m, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	seq, data, err := DataSeq(m)
	if err != nil {
		t.Fatalf("DataSeq: %v", err)
	}
	if seq != 1<<33 {
		t.Fatalf("seq = %d, want %d", seq, 1<<33)
	}
	if string(data) != "payload" {
		t.Fatalf("data = %q, want %q", data, "payload")
	}

	// Empty data payload: exactly the 8-byte sequence header.
	buf.Reset()
	if err := WriteData(&buf, 5, nil); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	m, err = ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	seq, data, err = DataSeq(m)
	if err != nil {
		t.Fatalf("DataSeq: %v", err)
	}
	if seq != 5 || len(data) != 0 {
		t.Fatalf("empty data = (%d, %q), want (5, empty)", seq, data)
	}
}

func TestDataSeqErrors(t *testing.T) {
	if _, _, err := DataSeq(Message{Type: TypePing}); !errors.Is(err, ErrBadType) {
		t.Fatalf("wrong type: err = %v, want ErrBadType", err)
	}
	for _, n := range []int{0, 4, 7} {
		if _, _, err := DataSeq(Message{Type: TypeData, Payload: make([]byte, n)}); !errors.Is(err, ErrShortPayload) {
			t.Fatalf("len %d: err = %v, want ErrShortPayload", n, err)
		}
	}
}

func TestSeqOf(t *testing.T) {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, 987654321)
	seq, err := SeqOf(Message{Type: TypeAck, Payload: payload})
	if err != nil {
		t.Fatalf("SeqOf: %v", err)
	}
	if seq != 987654321 {
		t.Fatalf("seq = %d, want 987654321", seq)
	}

	if _, err := SeqOf(Message{Type: TypePing, Payload: payload}); !errors.Is(err, ErrBadType) {
		t.Fatalf("wrong type: err = %v, want ErrBadType", err)
	}
	for _, n := range []int{0, 7, 9} {
		if _, err := SeqOf(Message{Type: TypeAck, Payload: make([]byte, n)}); !errors.Is(err, ErrShortPayload) {
			t.Fatalf("len %d: err = %v, want ErrShortPayload", n, err)
		}
	}
}

func TestExitCodeAndOverrunRange(t *testing.T) {
	exitPayload := make([]byte, 4)
	binary.BigEndian.PutUint32(exitPayload, uint32(130))
	code, err := ExitCode(Message{Type: TypeExit, Payload: exitPayload})
	if err != nil || code != 130 {
		t.Fatalf("ExitCode = (%d, %v), want (130, nil)", code, err)
	}
	// Signed decoding.
	binary.BigEndian.PutUint32(exitPayload, 0xFFFFFFFF)
	if code, err = ExitCode(Message{Type: TypeExit, Payload: exitPayload}); err != nil || code != -1 {
		t.Fatalf("ExitCode = (%d, %v), want (-1, nil)", code, err)
	}

	overrunPayload := make([]byte, 16)
	binary.BigEndian.PutUint64(overrunPayload[0:8], 11)
	binary.BigEndian.PutUint64(overrunPayload[8:16], 21)
	from, to, err := OverrunRange(Message{Type: TypeOverrun, Payload: overrunPayload})
	if err != nil || from != 11 || to != 21 {
		t.Fatalf("OverrunRange = (%d, %d, %v), want (11, 21, nil)", from, to, err)
	}
}

func TestEmptyPayloadTypes(t *testing.T) {
	for _, typ := range []Type{TypeDetach, TypeKill, TypePing, TypePong} {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, Message{Type: typ}); err != nil {
			t.Fatalf("%s: WriteMessage: %v", typ, err)
		}
		if buf.Len() != HeaderSize {
			t.Fatalf("%s: encoded length = %d, want %d", typ, buf.Len(), HeaderSize)
		}
		m, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("%s: ReadMessage: %v", typ, err)
		}
		if m.Type != typ {
			t.Fatalf("type = %d, want %d", m.Type, typ)
		}
		if len(m.Payload) != 0 {
			t.Fatalf("%s: payload = %q, want empty", typ, m.Payload)
		}
	}
}

func TestTypeString(t *testing.T) {
	want := map[Type]string{
		TypeHello:   "hello",
		TypeData:    "data",
		TypeResize:  "resize",
		TypeAck:     "ack",
		TypeDetach:  "detach",
		TypeExit:    "exit",
		TypePing:    "ping",
		TypePong:    "pong",
		TypeOverrun: "overrun",
		TypeError:   "error",
		TypeKill:    "kill",
	}
	for typ, s := range want {
		if got := typ.String(); got != s {
			t.Fatalf("%d.String() = %q, want %q", typ, got, s)
		}
	}
	if got := Type(200).String(); got != "unknown" {
		t.Fatalf("Type(200).String() = %q, want \"unknown\"", got)
	}
}

// writeCounter records how many Write calls a frame used.
type writeCounter struct {
	calls int
	data  []byte
}

func (w *writeCounter) Write(p []byte) (int, error) {
	w.calls++
	w.data = append(w.data, p...)
	return len(p), nil
}
