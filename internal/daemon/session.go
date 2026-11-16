//go:build !windows

// Package daemon implements the remote half of tether: a detached supervisor
// that owns the PTY, a sequence-numbered ring buffer and the Unix socket a
// client attaches to.
//
// One Session corresponds to one shell. The supervisor is deliberately dumb: it
// never interprets terminal output, it only copies PTY bytes into the ring
// buffer, numbers them, and fans them out to attached clients.
package daemon

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/naseer-hussain/tether/internal/proto"
	"github.com/naseer-hussain/tether/internal/ringbuf"
	"github.com/naseer-hussain/tether/internal/version"
)

const (
	// pumpChunkSize bounds one read from the PTY master.
	pumpChunkSize = 32 << 10
	// replayChunkSize bounds the payload of one replay Data frame.
	replayChunkSize = 32 << 10
	// clientQueue is the number of output chunks buffered per attached client.
	// A client that falls further behind is disconnected: the ring buffer is
	// the only unbounded-growth risk we refuse to take.
	clientQueue = 64
	// helloTimeout is how long a fresh connection has to send a valid Hello.
	helloTimeout = 5 * time.Second
	// writeTimeout bounds a single frame write to a client, so a wedged peer
	// cannot pin a goroutine forever.
	writeTimeout = 2 * time.Second
	// detachDrain is how long a Detach waits for queued output to flush.
	detachDrain = 200 * time.Millisecond
	// exitGrace is how long shutdown waits for clients to read the Exit frame.
	exitGrace = 2 * time.Second
	// termGrace is how long Close waits between SIGTERM and SIGKILL.
	termGrace = 3 * time.Second
	// killGrace is how long a client-initiated Kill waits before SIGKILL.
	killGrace = 5 * time.Second
	// maxErrorSize keeps TypeData/TypeError payloads comfortably small.
	maxErrorSize = 4 << 10
)

// sessionMeta is the <id>.json shape. It is written by the supervisor and read
// by List; corrupt files are ignored.
type sessionMeta struct {
	ID         string `json:"id"`
	PID        int    `json:"pid"`
	Shell      string `json:"shell"`
	BufferSize int    `json:"buffer_size"`
	Rows       uint16 `json:"rows"`
	Cols       uint16 `json:"cols"`
	Started    string `json:"started"`
}

// Session is a running shell plus its PTY, ring buffer and socket listener.
type Session struct {
	cfg  Config
	ring *ringbuf.Buffer

	ptmx *os.File
	cmd  *exec.Cmd
	ln   net.Listener

	// ptyMu serialises writes and window-size changes on the PTY master.
	ptyMu sync.Mutex

	mu       sync.Mutex
	clients  map[*client]struct{}
	closed   bool
	exitCode int32

	// done is closed once the PTY pump has reaped the shell.
	done         chan struct{}
	stopOnce     sync.Once
	shutdownOnce sync.Once
	killOnce     sync.Once

	logger  *log.Logger
	logFile *os.File
}

// New starts the shell on a fresh PTY, creates the session socket and metadata,
// and begins pumping PTY output into the ring buffer. Serve must be called to
// accept clients.
func New(cfg Config) (*Session, error) {
	if err := cfg.Normalize(); err != nil {
		return nil, err
	}

	s := &Session{
		cfg:     cfg,
		ring:    ringbuf.New(cfg.BufferSize),
		clients: make(map[*client]struct{}),
		done:    make(chan struct{}),
	}
	if err := s.openLog(); err != nil {
		return nil, err
	}

	cmd := exec.Command(cfg.Shell, cfg.ShellArgs...)
	if len(cfg.Env) > 0 {
		cmd.Env = cfg.Env
	} else {
		cmd.Env = os.Environ()
	}
	if home, err := os.UserHomeDir(); err == nil {
		cmd.Dir = home
	}

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: cfg.Rows, Cols: cfg.Cols})
	if err != nil {
		s.closeLog()
		return nil, fmt.Errorf("daemon: start shell %q: %w", cfg.Shell, err)
	}
	s.cmd = cmd
	s.ptmx = ptmx

	ln, err := listenUnix(cfg.SocketPath)
	if err != nil {
		s.abortStart()
		return nil, err
	}
	s.ln = ln

	if err := s.writeMetadata(); err != nil {
		s.abortStart()
		return nil, err
	}

	s.logf("session %s started: pid=%d shell=%q socket=%s buffer=%d rows=%d cols=%d",
		cfg.SessionID, cmd.Process.Pid, cfg.Shell, cfg.SocketPath, cfg.BufferSize, cfg.Rows, cfg.Cols)
	go s.pump()
	return s, nil
}

// abortStart unwinds a partially constructed session.
func (s *Session) abortStart() {
	if s.ln != nil {
		_ = s.ln.Close()
	}
	if s.ptmx != nil {
		_ = s.ptmx.Close()
	}
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_, _ = s.cmd.Process.Wait()
	}
	s.removeFiles()
	s.closeLog()
}

// ID returns the session id.
func (s *Session) ID() string { return s.cfg.SessionID }

// SocketPath returns the path of the session's Unix socket.
func (s *Session) SocketPath() string { return s.cfg.SocketPath }

// PID returns the shell's process id, or 0 before it is started.
func (s *Session) PID() int {
	if s.cmd == nil || s.cmd.Process == nil {
		return 0
	}
	return s.cmd.Process.Pid
}

// Head returns the sequence number of the newest byte in the ring buffer.
func (s *Session) Head() uint64 { return s.ring.Head() }

// Attached returns the number of live client connections.
func (s *Session) Attached() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

// Serve accepts clients until the shell exits or ctx is cancelled. It returns
// nil when the shell exited, and ctx.Err() when the context ended the session.
func (s *Session) Serve(ctx context.Context) error {
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		s.acceptLoop()
	}()

	var err error
	select {
	case <-s.done:
	case <-ctx.Done():
		err = ctx.Err()
		_ = s.Close()
	}

	s.shutdown()
	<-acceptDone
	return err
}

// Close terminates the shell (SIGTERM, then SIGKILL after three seconds) and
// removes the session's socket and metadata. It is safe to call more than once
// and concurrently with Serve.
func (s *Session) Close() error {
	s.stopOnce.Do(func() {
		s.terminate()
		s.shutdown()
	})
	return nil
}

// pump copies PTY output into the ring buffer and fans it out to clients. It
// owns cmd.Wait: PTY EOF or EIO means the shell is gone.
func (s *Session) pump() {
	defer close(s.done)

	buf := make([]byte, pumpChunkSize)
	for {
		n, err := s.ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			_, _ = s.ring.Write(data)
			first := s.ring.Head() - uint64(n) + 1
			s.broadcast(chunk{seq: first, data: data})
		}
		if err != nil {
			break
		}
	}

	code := s.reap()
	s.mu.Lock()
	s.exitCode = code
	s.mu.Unlock()
	s.logf("session %s: shell exited with code %d", s.cfg.SessionID, code)
}

// reap waits for the child and reports its exit status per os.ProcessState.
func (s *Session) reap() int32 {
	if s.cmd == nil {
		return -1
	}
	_ = s.cmd.Wait()
	if s.cmd.ProcessState != nil {
		return int32(s.cmd.ProcessState.ExitCode())
	}
	return -1
}

// terminate signals the shell's process group and closes the PTY master.
func (s *Session) terminate() {
	if s.cmd != nil && s.cmd.Process != nil {
		pid := s.cmd.Process.Pid
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		select {
		case <-s.done:
		case <-time.After(termGrace):
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			select {
			case <-s.done:
			case <-time.After(time.Second):
			}
		}
	}
	if s.ptmx != nil {
		// Closing the master unblocks the pump even if a grandchild still holds
		// the slave open.
		_ = s.ptmx.Close()
	}
	select {
	case <-s.done:
	case <-time.After(time.Second):
	}
}

// acceptLoop admits connections until the listener is closed.
func (s *Session) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				s.logf("session %s: accept: %v", s.cfg.SessionID, err)
			}
			return
		}
		go s.handleConn(conn)
	}
}

// shutdown stops admitting clients, tells the attached ones the shell exited
// and removes the session's files. The log file is kept for post-mortem.
func (s *Session) shutdown() {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		code := s.exitCode
		attached := make([]*client, 0, len(s.clients))
		for c := range s.clients {
			attached = append(attached, c)
		}
		s.mu.Unlock()

		frame := exitFrame(code)
		for _, c := range attached {
			c.enqueueFinal(outMsg{frame: &frame})
		}
		s.waitClients(exitGrace)

		if s.ln != nil {
			_ = s.ln.Close()
		}

		s.mu.Lock()
		remaining := make([]*client, 0, len(s.clients))
		for c := range s.clients {
			remaining = append(remaining, c)
		}
		s.mu.Unlock()
		for _, c := range remaining {
			c.close()
		}

		s.removeFiles()
		s.closeLog()
	})
}

// waitClients waits until no client is registered, or the timeout expires.
func (s *Session) waitClients(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		n := len(s.clients)
		s.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// requestKill implements the client-initiated TypeKill: SIGTERM the process
// group, then SIGKILL after five seconds.
func (s *Session) requestKill() {
	s.killOnce.Do(func() {
		if s.cmd == nil || s.cmd.Process == nil {
			return
		}
		pid := s.cmd.Process.Pid
		s.logf("session %s: kill requested", s.cfg.SessionID)
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		go func() {
			select {
			case <-s.done:
				return
			case <-time.After(killGrace):
			}
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}()
	})
}

// resize applies a window size and notifies the child process group.
func (s *Session) resize(rows, cols uint16) {
	if rows == 0 || cols == 0 {
		return
	}
	s.ptyMu.Lock()
	defer s.ptyMu.Unlock()
	if s.ptmx == nil {
		return
	}
	if err := pty.Setsize(s.ptmx, &pty.Winsize{Rows: rows, Cols: cols}); err != nil {
		s.logf("session %s: resize %dx%d: %v", s.cfg.SessionID, rows, cols, err)
		return
	}
	if s.cmd != nil && s.cmd.Process != nil {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGWINCH)
	}
}

// writePTY forwards client input to the shell.
func (s *Session) writePTY(p []byte) {
	s.ptyMu.Lock()
	defer s.ptyMu.Unlock()
	if s.ptmx == nil {
		return
	}
	_, _ = s.ptmx.Write(p)
}

// broadcast hands one PTY chunk to every attached client without blocking.
func (s *Session) broadcast(ch chunk) {
	s.mu.Lock()
	cs := make([]*client, 0, len(s.clients))
	for c := range s.clients {
		cs = append(cs, c)
	}
	s.mu.Unlock()
	for _, c := range cs {
		c.enqueue(outMsg{seq: ch.seq, data: ch.data})
	}
}

// register adds c to the attached set, or reports false while shutting down.
func (s *Session) register(c *client) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.clients[c] = struct{}{}
	return true
}

func (s *Session) unregister(c *client) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()
}

// handleConn performs the attach handshake, replays the ring buffer and then
// relays frames until the connection ends.
func (s *Session) handleConn(conn net.Conn) {
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(helloTimeout)); err != nil {
		return
	}
	msg, err := proto.ReadMessage(conn)
	if err != nil {
		return
	}
	if msg.Type == proto.TypeKill {
		// A bare Kill frame is accepted before Hello so `tetherd kill` does not
		// have to become a full client.
		s.requestKill()
		return
	}
	if msg.Type != proto.TypeHello {
		s.reject(conn, fmt.Sprintf("expected hello frame, got %s", msg.Type))
		return
	}
	var hello proto.Hello
	if err := hello.UnmarshalBinary(msg.Payload); err != nil {
		s.reject(conn, "malformed hello frame")
		return
	}
	if hello.Version != version.ProtocolVersion {
		s.reject(conn, fmt.Sprintf("protocol version %d not supported (daemon speaks %d)",
			hello.Version, version.ProtocolVersion))
		return
	}

	// The hello geometry is applied before any replay so full-screen programs
	// redraw at the right size.
	if hello.Rows > 0 && hello.Cols > 0 {
		s.resize(hello.Rows, hello.Cols)
	}

	c := &client{
		sess: s,
		conn: conn,
		out:  make(chan outMsg, clientQueue),
		done: make(chan struct{}),
	}
	if !s.register(c) {
		s.mu.Lock()
		code := s.exitCode
		s.mu.Unlock()
		frame := exitFrame(code)
		_ = proto.WriteMessage(conn, frame)
		return
	}
	defer func() {
		s.unregister(c)
		c.close()
	}()

	data, start, overrun := s.ring.ReadFrom(hello.LastAckSeq)
	c.next = start
	if overrun {
		from := hello.LastAckSeq + 1
		if from == 0 {
			from = 1
		}
		if start > from {
			frame := overrunFrame(from, start)
			if err := proto.WriteMessage(conn, frame); err != nil {
				return
			}
		}
	}
	for off := 0; off < len(data); off += replayChunkSize {
		end := min(off+replayChunkSize, len(data))
		if err := c.write(outMsg{seq: start + uint64(off), data: data[off:end]}); err != nil {
			return
		}
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	go c.writeLoop()

	for {
		msg, err := proto.ReadMessage(conn)
		if err != nil {
			return
		}
		switch msg.Type {
		case proto.TypeData:
			if _, p, derr := proto.DataSeq(msg); derr == nil && len(p) > 0 {
				s.writePTY(p)
			}
		case proto.TypeResize:
			var r proto.Resize
			if err := r.UnmarshalBinary(msg.Payload); err == nil {
				s.resize(r.Rows, r.Cols)
			}
		case proto.TypeAck:
			if seq, err := proto.SeqOf(msg); err == nil {
				c.wmu.Lock()
				if seq > c.ack {
					c.ack = seq
				}
				c.wmu.Unlock()
			}
		case proto.TypePing:
			c.enqueue(outMsg{frame: &proto.Message{Type: proto.TypePong}})
		case proto.TypeDetach:
			c.drain(detachDrain)
			return
		case proto.TypeKill:
			s.requestKill()
		}
	}
}

// reject sends a human-readable TypeError and closes the connection.
func (s *Session) reject(conn net.Conn, text string) {
	if len(text) > maxErrorSize {
		text = text[:maxErrorSize]
	}
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	_ = proto.WriteMessage(conn, proto.Message{Type: proto.TypeError, Payload: []byte(text)})
	s.logf("session %s: attach rejected: %s", s.cfg.SessionID, text)
}

// client is one attached connection. All frame writes are serialised by wmu;
// the replay happens before the write loop starts, so in practice the loop is
// the only writer afterwards.
type client struct {
	sess *Session
	conn net.Conn
	out  chan outMsg
	done chan struct{}
	once sync.Once

	wmu  sync.Mutex
	ack  uint64
	next uint64 // sequence number of the next byte to send
}

// chunk is one read from the PTY, ready for fan-out.
type chunk struct {
	seq  uint64
	data []byte
}

// outMsg is either a Data chunk (frame == nil) or an explicit control frame.
type outMsg struct {
	seq   uint64
	data  []byte
	frame *proto.Message
	final bool
}

// close shuts the connection down exactly once.
func (c *client) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

// enqueue offers m to the writer without blocking. A full queue means the peer
// cannot keep up: drop the attachment, the client replays on reconnect.
func (c *client) enqueue(m outMsg) {
	select {
	case <-c.done:
	case c.out <- m:
	default:
		c.sess.logf("session %s: dropping slow client", c.sess.cfg.SessionID)
		c.close()
	}
}

// enqueueFinal marks m as the last frame the writer will send.
func (c *client) enqueueFinal(m outMsg) {
	m.final = true
	select {
	case <-c.done:
	case c.out <- m:
	default:
		c.close()
	}
}

// write emits one message. Data chunks below c.next were already sent (they are
// part of the replay snapshot) and are trimmed away.
func (c *client) write(m outMsg) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if m.frame != nil {
		return proto.WriteMessage(c.conn, *m.frame)
	}
	if len(m.data) == 0 {
		return nil
	}

	seq := m.seq
	p := m.data
	if end := seq + uint64(len(p)); end <= c.next {
		return nil
	}
	if seq < c.next {
		p = p[int(c.next-seq):]
		seq = c.next
	}
	if err := proto.WriteData(c.conn, seq, p); err != nil {
		return err
	}
	c.next = seq + uint64(len(p))
	return nil
}

// writeLoop drains out until the connection dies or a final frame is written.
func (c *client) writeLoop() {
	defer c.close()
	for {
		select {
		case m := <-c.out:
			if err := c.write(m); err != nil {
				return
			}
			if m.final {
				return
			}
		case <-c.done:
			return
		}
	}
}

// drain waits briefly for queued output to reach the socket before a clean
// detach closes it.
func (c *client) drain(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for len(c.out) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(5 * time.Millisecond)
}

// listenUnix creates the session socket, replacing a stale one. A socket that
// still accepts connections belongs to a live supervisor and is not removed.
func listenUnix(path string) (net.Listener, error) {
	if _, err := os.Stat(path); err == nil {
		if conn, derr := net.DialTimeout("unix", path, 200*time.Millisecond); derr == nil {
			_ = conn.Close()
			return nil, fmt.Errorf("daemon: session socket %s is already in use", path)
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("daemon: remove stale socket %s: %w", path, err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("daemon: listen %s: %w", path, err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("daemon: chmod socket %s: %w", path, err)
	}
	return ln, nil
}

// writeMetadata records the pid and the machine-readable session description.
func (s *Session) writeMetadata() error {
	pidFile := pidPath(s.cfg)
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(s.cmd.Process.Pid)+"\n"), 0600); err != nil {
		return fmt.Errorf("daemon: write %s: %w", pidFile, err)
	}
	meta := sessionMeta{
		ID:         s.cfg.SessionID,
		PID:        s.cmd.Process.Pid,
		Shell:      s.cfg.Shell,
		BufferSize: s.cfg.BufferSize,
		Rows:       s.cfg.Rows,
		Cols:       s.cfg.Cols,
		Started:    time.Now().UTC().Format(time.RFC3339),
	}
	buf, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("daemon: encode metadata: %w", err)
	}
	buf = append(buf, '\n')
	if err := os.WriteFile(metaPath(s.cfg), buf, 0600); err != nil {
		return fmt.Errorf("daemon: write %s: %w", metaPath(s.cfg), err)
	}
	return nil
}

// removeFiles deletes the socket, pid file and metadata; the log survives.
func (s *Session) removeFiles() {
	_ = os.Remove(s.cfg.SocketPath)
	_ = os.Remove(pidPath(s.cfg))
	_ = os.Remove(metaPath(s.cfg))
}

func (s *Session) openLog() error {
	if s.cfg.LogPath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.cfg.LogPath), 0700); err != nil {
		return fmt.Errorf("daemon: create log directory: %w", err)
	}
	f, err := os.OpenFile(s.cfg.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("daemon: open log %s: %w", s.cfg.LogPath, err)
	}
	s.logFile = f
	s.logger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	return nil
}

func (s *Session) closeLog() {
	if s.logFile != nil {
		_ = s.logFile.Close()
		s.logFile = nil
		s.logger = nil
	}
}

func (s *Session) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

// exitFrame builds a TypeExit message.
func exitFrame(code int32) proto.Message {
	p := make([]byte, 4)
	binary.BigEndian.PutUint32(p, uint32(code))
	return proto.Message{Type: proto.TypeExit, Payload: p}
}

// overrunFrame builds a TypeOverrun message for the half-open range [from, to).
func overrunFrame(from, to uint64) proto.Message {
	p := make([]byte, 16)
	binary.BigEndian.PutUint64(p[0:8], from)
	binary.BigEndian.PutUint64(p[8:16], to)
	return proto.Message{Type: proto.TypeOverrun, Payload: p}
}
