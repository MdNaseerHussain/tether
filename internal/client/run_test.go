package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/naseer-hussain/tether/internal/proto"
	"github.com/naseer-hussain/tether/internal/ringbuf"
	"github.com/naseer-hussain/tether/internal/version"
)

func TestLastAckFor(t *testing.T) {
	cases := []struct {
		firstSeq uint64
		n        int
		want     uint64
	}{
		{1, 1, 1},
		{1, 3, 3},
		{100, 0, 99},
		{0, 0, 0},
		{42, 2, 43},
		{1 << 32, 1 << 16, (1 << 32) + (1 << 16) - 1},
	}
	for _, tc := range cases {
		if got := lastAckFor(tc.firstSeq, tc.n); got != tc.want {
			t.Errorf("lastAckFor(%d, %d) = %d, want %d", tc.firstSeq, tc.n, got, tc.want)
		}
	}
}

func TestExitError(t *testing.T) {
	var err error = &ExitError{Code: 130}

	var exitErr *ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("errors.As failed for %T", err)
	}
	if got := exitErr.ExitCode(); got != 130 {
		t.Fatalf("ExitCode() = %d, want 130", got)
	}
	if !strings.Contains(err.Error(), "130") {
		t.Fatalf("error text %q does not mention the status", err.Error())
	}
}

func TestRemoteCommand(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{
			name: "defaults",
			opts: Options{Session: "default", RemoteBin: "tetherd"},
			want: "tetherd attach --session default --spawn",
		},
		{
			name: "buffer size only when positive",
			opts: Options{Session: "abc12345", RemoteBin: "tetherd", BufferSize: 1 << 20},
			want: "tetherd attach --session abc12345 --spawn --buffer-size 1048576",
		},
		{
			name: "path with spaces is quoted",
			opts: Options{Session: "dev box", RemoteBin: "/opt/my tether/bin/tetherd"},
			want: `'/opt/my tether/bin/tetherd' attach --session 'dev box' --spawn`,
		},
		{
			name: "single quotes are escaped",
			opts: Options{Session: "it's", RemoteBin: "tetherd"},
			want: `tetherd attach --session 'it'\''s' --spawn`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := remoteCommand(tc.opts); got != tc.want {
				t.Fatalf("remoteCommand = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestShellQuote(t *testing.T) {
	cases := map[string]string{
		"tetherd":          "tetherd",
		"/usr/local/bin/x": "/usr/local/bin/x",
		"./bin/tetherd":    "./bin/tetherd",
		"a b":              "'a b'",
		"":                 "''",
		"a;b":              "'a;b'",
		"$HOME":            "'$HOME'",
		"it's":             `'it'\''s'`,
	}
	for in, want := range cases {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q) = %q, want %q", in, got, want)
		}
	}
}

// syncBuffer is a bytes.Buffer safe for the reader goroutine and the test.
type syncBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// pipeChannel adapts one end of a net.Pipe to the channel interface.
type pipeChannel struct {
	net.Conn
}

func (p *pipeChannel) Wait() error { return nil }

func TestRunWithFakeDaemon(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	stdinR, stdinW := io.Pipe()
	defer stdinW.Close()

	var out syncBuffer
	opts := Options{
		User:    "tester",
		Host:    "example.test",
		Port:    22,
		Session: "default",
		// Escape is disabled here so the test cannot depend on the detach
		// parser; it is covered separately.
		Escape:         "none",
		RemoteBin:      "tetherd",
		ConnectTimeout: time.Second,
		Stdin:          stdinR,
		Stdout:         &out,
		Stderr:         io.Discard,
	}.withDefaults()

	dial := func(ctx context.Context) (channel, error) {
		return &pipeChannel{Conn: clientConn}, nil
	}

	daemonErr := make(chan error, 1)
	go func() { daemonErr <- fakeDaemon(serverConn) }()

	errCh := make(chan error, 1)
	go func() { errCh <- runWith(context.Background(), opts, dial) }()

	// "hi" is forwarded as one Data frame once Hello and Resize are out.
	if _, err := stdinW.Write([]byte("hi")); err != nil {
		t.Fatalf("stdin write: %v", err)
	}

	select {
	case err := <-errCh:
		var exitErr *ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("runWith = %v, want *ExitError", err)
		}
		if exitErr.Code != 0 {
			t.Fatalf("exit code = %d, want 0", exitErr.Code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runWith did not return")
	}

	want := ringbuf.NoticeOverrun + "\r\nhello from daemon\r\n"
	if got := out.String(); got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}

	if err := <-daemonErr; err != nil {
		t.Fatalf("fake daemon: %v", err)
	}
}

// fakeDaemon plays the remote tetherd bridge: it validates Hello and Resize,
// replays an overrun notice plus data, and expects the client's input before
// reporting a clean exit.
func fakeDaemon(c net.Conn) error {
	defer c.Close()

	m, err := proto.ReadMessage(c)
	if err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if m.Type != proto.TypeHello {
		return fmt.Errorf("first frame = %s, want hello", m.Type)
	}
	var hello proto.Hello
	if err := hello.UnmarshalBinary(m.Payload); err != nil {
		return fmt.Errorf("decode hello: %w", err)
	}
	if hello.Version != version.ProtocolVersion {
		return fmt.Errorf("hello version = %d, want %d", hello.Version, version.ProtocolVersion)
	}
	if hello.LastAckSeq != 0 {
		return fmt.Errorf("hello lastAckSeq = %d, want 0", hello.LastAckSeq)
	}
	if hello.Rows == 0 || hello.Cols == 0 {
		return fmt.Errorf("hello size = %dx%d", hello.Rows, hello.Cols)
	}

	m, err = proto.ReadMessage(c)
	if err != nil {
		return fmt.Errorf("read resize: %w", err)
	}
	if m.Type != proto.TypeResize {
		return fmt.Errorf("second frame = %s, want resize", m.Type)
	}
	var size proto.Resize
	if err := size.UnmarshalBinary(m.Payload); err != nil {
		return fmt.Errorf("decode resize: %w", err)
	}
	if size.Rows != hello.Rows || size.Cols != hello.Cols {
		return fmt.Errorf("resize %dx%d differs from hello %dx%d", size.Rows, size.Cols, hello.Rows, hello.Cols)
	}

	// A dropped range, then the replay.
	overrun := make([]byte, 16)
	binary.BigEndian.PutUint64(overrun[0:8], 1)
	binary.BigEndian.PutUint64(overrun[8:16], 2)
	if err := proto.WriteMessage(c, proto.Message{Type: proto.TypeOverrun, Payload: overrun}); err != nil {
		return fmt.Errorf("write overrun: %w", err)
	}
	if err := proto.WriteData(c, 2, []byte("hello from daemon\r\n")); err != nil {
		return fmt.Errorf("write data: %w", err)
	}

	for {
		m, err := proto.ReadMessage(c)
		if err != nil {
			return fmt.Errorf("read client frame: %w", err)
		}
		switch m.Type {
		case proto.TypePing:
			if err := proto.WriteMessage(c, proto.Message{Type: proto.TypePong}); err != nil {
				return fmt.Errorf("write pong: %w", err)
			}
		case proto.TypeData:
			seq, p, err := proto.DataSeq(m)
			if err != nil {
				return fmt.Errorf("decode client data: %w", err)
			}
			if seq != 0 {
				return fmt.Errorf("client data seq = %d, want 0", seq)
			}
			if string(p) != "hi" {
				return fmt.Errorf("client data = %q, want %q", p, "hi")
			}
			code := make([]byte, 4)
			binary.BigEndian.PutUint32(code, 0)
			return proto.WriteMessage(c, proto.Message{Type: proto.TypeExit, Payload: code})
		case proto.TypeAck:
			if _, err := proto.SeqOf(m); err != nil {
				return fmt.Errorf("decode ack: %w", err)
			}
		}
	}
}
