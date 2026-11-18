package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"os/user"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/MdNaseerHussain/tether/internal/proto"
	"github.com/MdNaseerHussain/tether/internal/ringbuf"
	"github.com/MdNaseerHussain/tether/internal/version"
	"golang.org/x/term"
)

// Options configures Run. Zero values select the documented defaults, except
// Host which is required.
type Options struct {
	User           string
	Host           string
	Port           int
	IdentityFile   string
	KnownHostsFile string
	Insecure       bool
	Session        string
	RemoteBin      string
	BufferSize     int
	Escape         string
	ConnectTimeout time.Duration
	Verbose        bool

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

const (
	defaultPort       = 22
	defaultSession    = "default"
	defaultRemoteBin  = "tetherd"
	defaultEscape     = "~."
	defaultKnownHosts = "~/.ssh/known_hosts"

	defaultConnectTimeout = 10 * time.Second

	// inputChunk is the largest client -> server Data payload. Keystrokes and
	// pastes are tiny; splitting at 16 KiB keeps frames well under the 1 MiB
	// protocol limit while avoiding per-byte overhead.
	inputChunk = 16 << 10

	// ackEvery bounds Ack traffic. The server only needs it to replay from a
	// sane cursor after a reconnect.
	ackEvery = 250 * time.Millisecond

	// heartbeatEvery/heartbeatLimit match docs/PROTOCOL.md §6: ping every 3 s and
	// declare the connection dead after 9 s without an inbound frame.
	heartbeatEvery = 3 * time.Second
	heartbeatLimit = 9 * time.Second

	// livenessEvery is how often the dead-peer deadline is evaluated. It is
	// deliberately much smaller than heartbeatEvery: detection must not be
	// quantised to the ping cadence, or three missed heartbeats would only be
	// noticed on the next 3 s tick (up to 12 s instead of the promised 9 s).
	livenessEvery = 250 * time.Millisecond

	reconnectInitial = 500 * time.Millisecond
	reconnectMax     = 5 * time.Second

	// abortWindow is how close two Ctrl+C presses must be, while disconnected, to
	// abort the retry loop.
	abortWindow = 2 * time.Second

	// detachGrace is how long a detach waits for the daemon to close the channel
	// before the client closes it itself.
	detachGrace = 500 * time.Millisecond

	inputQueue = 16
)

// ExitError reports that the remote child process exited with Code. It lets the
// CLI mirror the status without printing a second error message.
type ExitError struct{ Code int32 }

func (e *ExitError) Error() string {
	return fmt.Sprintf("tether: remote session exited with status %d", e.Code)
}

// ExitCode returns the process status to exit with.
func (e *ExitError) ExitCode() int { return int(e.Code) }

// ErrRemoteMissing reports that the remote shell exited with status 127 before
// the protocol started, which means the attach binary is not installed.
type ErrRemoteMissing struct{ Host string }

func (e *ErrRemoteMissing) Error() string {
	return fmt.Sprintf("tether: tetherd not found on %s; install tetherd or pass --remote-bin <path>", e.Host)
}

// Run attaches to (or creates) the remote session and proxies the local terminal
// until the remote child exits, the user detaches, or ctx is cancelled.
func Run(ctx context.Context, opts Options) error {
	opts = opts.withDefaults()
	return runWith(ctx, opts, func(ctx context.Context) (channel, error) {
		return newTransport(ctx, opts)
	})
}

func (o Options) withDefaults() Options {
	if o.User == "" {
		if u, err := user.Current(); err == nil {
			o.User = u.Username
		}
	}
	if o.Port == 0 {
		o.Port = defaultPort
	}
	if o.Session == "" {
		o.Session = defaultSession
	}
	if o.RemoteBin == "" {
		o.RemoteBin = defaultRemoteBin
	}
	if o.Escape == "" {
		o.Escape = defaultEscape
	}
	if o.KnownHostsFile == "" {
		o.KnownHostsFile = defaultKnownHosts
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = defaultConnectTimeout
	}
	if o.Stdin == nil {
		o.Stdin = os.Stdin
	}
	if o.Stdout == nil {
		o.Stdout = os.Stdout
	}
	if o.Stderr == nil {
		o.Stderr = os.Stderr
	}
	return o
}

func (o Options) logf(format string, args ...any) {
	if o.Verbose {
		fmt.Fprintf(o.Stderr, "tether: "+format+"\n", args...)
	}
}

// runWith is Run with an injected dialer so the protocol loop can be tested over
// an in-memory pipe.
func runWith(ctx context.Context, opts Options, dial dialFunc) error {
	det, err := NewDetacher(opts.Escape)
	if err != nil {
		return err
	}

	if opts.Insecure {
		fmt.Fprintf(opts.Stderr, "tether: WARNING: host key verification is disabled (--insecure)\n")
	}

	tty, err := newTerminal(opts)
	if err != nil {
		return err
	}
	defer tty.restore()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	defer signal.Stop(winch)

	// Terminal signals are handled here; the raw-mode byte path handles SIGINT
	// itself (see the per-attempt registration below).
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	defer signal.Stop(sigCh)

	holder := &connHolder{}

	var aborted atomic.Bool
	abort := func() {
		aborted.Store(true)
		cancel()
	}

	var connected atomic.Bool
	inputCh := make(chan []byte, inputQueue)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sigCh:
				cancel()
				holder.forceClose()
				return
			}
		}
	}()

	// The stdin pump is deliberately not part of wg: a blocking Read on a
	// terminal cannot be cancelled portably, and Run must not wait for the next
	// keystroke before returning.
	go stdinPump(ctx, opts.Stdin, inputCh, &connected, abort)

	backoff := NewBackoff(reconnectInitial, reconnectMax, rand.New(rand.NewSource(time.Now().UnixNano())))
	rows, cols := tty.size()

	var lastAck atomic.Uint64
	attempt := 0
	lastDialErr := ""

	for ctx.Err() == nil {
		attempt++

		ch, dialErr := dial(ctx)
		if dialErr != nil {
			if ctx.Err() != nil {
				break
			}
			var hostKeyErr *HostKeyError
			if errors.As(dialErr, &hostKeyErr) {
				return dialErr
			}
			msg := dialErr.Error()
			if opts.Verbose || msg != lastDialErr {
				tty.statusf("tether: %s; retrying", strings.TrimPrefix(msg, "tether: "))
				lastDialErr = msg
			}
			if !sleepCtx(ctx, backoff.Next()) {
				break
			}
			continue
		}
		lastDialErr = ""

		holder.set(ch)
		connected.Store(true)

		// In raw mode the terminal driver never generates SIGINT, so registering
		// it for the duration of a connection only matters when the tty was not
		// made raw (pipes, --escape none edge cases) and is a cheap way to still
		// restore the terminal on Ctrl+C.
		done := make(chan struct{})
		intCh := make(chan os.Signal, 1)
		signal.Notify(intCh, os.Interrupt)
		go func() {
			select {
			case <-intCh:
				holder.forceClose()
			case <-done:
			}
		}()

		sess := &attach{
			ctx:       ctx,
			ch:        ch,
			opts:      opts,
			tty:       tty,
			det:       det,
			input:     inputCh,
			winch:     winch,
			ack:       &lastAck,
			w:         &frameWriter{w: ch},
			rows:      rows,
			cols:      cols,
			reconnect: attempt > 1,
		}
		sess.lastPong.Store(time.Now().UnixNano())

		res := runSession(sess)

		close(done)
		signal.Stop(intCh)
		connected.Store(false)
		holder.set(nil)

		if res.attached {
			// A working attach resets the backoff so a later blip retries in
			// 500 ms rather than at the cap.
			backoff.Reset()
		}

		switch res.kind {
		case resultDetach:
			tty.statusf("tether: detached from session %q", opts.Session)
			return nil
		case resultExit:
			return &ExitError{Code: res.code}
		case resultFatal:
			return res.err
		case resultCancelled:
			if aborted.Load() {
				return &ExitError{Code: 130}
			}
			cancel()
			wg.Wait()
			return nil
		}

		if ctx.Err() == nil {
			if res.err != nil {
				tty.statusf("tether: connection lost (%v); reconnecting", res.err)
			} else {
				tty.statusf("tether: connection lost; reconnecting")
			}
		}
		if !sleepCtx(ctx, backoff.Next()) {
			break
		}
	}

	cancel()
	wg.Wait()
	if aborted.Load() {
		return &ExitError{Code: 130}
	}
	return nil
}

// terminal owns the local tty: raw mode, restoration and window size. Exactly
// one restore happens, on every return path.
type terminal struct {
	in           io.Reader
	out          io.Writer
	err          io.Writer
	fd           int
	inFD         int
	raw          bool
	restoreState *term.State
	restoreOnce  sync.Once
}

func newTerminal(opts Options) (*terminal, error) {
	t := &terminal{in: opts.Stdin, out: opts.Stdout, err: opts.Stderr, fd: -1, inFD: -1}

	if f, ok := opts.Stdout.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		t.fd = int(f.Fd())
	}
	if f, ok := opts.Stdin.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		state, err := term.MakeRaw(int(f.Fd()))
		if err != nil {
			return nil, fmt.Errorf("tether: cannot put terminal in raw mode: %w", err)
		}
		t.raw = true
		t.restoreState = state
		t.inFD = int(f.Fd())
		if t.fd < 0 {
			t.fd = int(f.Fd())
		}
	}
	return t, nil
}

func (t *terminal) restore() {
	t.restoreOnce.Do(func() {
		if t.raw && t.restoreState != nil {
			_ = term.Restore(t.inFD, t.restoreState)
		}
	})
}

func (t *terminal) size() (rows, cols uint16) {
	if t.fd >= 0 {
		if w, h, err := term.GetSize(t.fd); err == nil && w > 0 && h > 0 {
			return uint16(h), uint16(w)
		}
	}
	return 24, 80
}

// statusf writes a status line to stderr, but only when stderr is a terminal:
// redirected stderr is reserved for real diagnostics and must stay parseable.
func (t *terminal) statusf(format string, args ...any) {
	if !isTTY(t.err) {
		return
	}
	msg := fmt.Sprintf(format, args...)
	if t.raw {
		fmt.Fprintf(t.err, "\r\n%s\r\n", msg)
		return
	}
	fmt.Fprintf(t.err, "%s\n", msg)
}

func isTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && term.IsTerminal(int(f.Fd()))
}

// connHolder publishes the transport currently in use so signal handlers can
// force it to fail.
type connHolder struct {
	mu sync.Mutex
	ch channel
}

func (h *connHolder) set(ch channel) {
	h.mu.Lock()
	h.ch = ch
	h.mu.Unlock()
}

func (h *connHolder) forceClose() {
	h.mu.Lock()
	ch := h.ch
	h.mu.Unlock()
	if ch == nil {
		return
	}
	_ = ch.SetDeadline(time.Now())
	_ = ch.Close()
}

// stdinPump reads the local input and feeds it to the current connection. While
// disconnected it instead watches for two Ctrl+C presses within abortWindow,
// which is the documented escape hatch out of an endless retry loop.
func stdinPump(ctx context.Context, r io.Reader, input chan<- []byte, connected *atomic.Bool, abort func()) {
	buf := make([]byte, inputChunk)
	var lastCtrlC time.Time

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := r.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])

			if connected.Load() {
				select {
				case input <- chunk:
				case <-ctx.Done():
					return
				default:
					// Keystrokes are never replayed, so a chunk that cannot be
					// handed over right now (a reconnect in progress) is dropped
					// rather than blocking the terminal.
				}
			} else {
				for _, b := range chunk {
					if b != 0x03 {
						continue
					}
					now := time.Now()
					if !lastCtrlC.IsZero() && now.Sub(lastCtrlC) <= abortWindow {
						abort()
						return
					}
					lastCtrlC = now
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// frameWriter serialises all protocol writes on one connection. proto does not
// lock, and two goroutines (reader replying Pong, writer sending Data) share the
// stream.
type frameWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (fw *frameWriter) write(m proto.Message) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return proto.WriteMessage(fw.w, m)
}

// writeData sends client input. firstSeq is always 0: input has no sequence
// space because it is never replayed (docs/PROTOCOL.md §4.1).
func (fw *frameWriter) writeData(p []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return proto.WriteData(fw.w, 0, p)
}

type resultKind int

const (
	resultReconnect resultKind = iota
	resultCancelled
	resultDetach
	resultExit
	resultFatal
)

type sessionResult struct {
	kind     resultKind
	err      error
	code     int32
	attached bool
}

// attach is the state of one connection attempt.
type attach struct {
	ctx   context.Context
	ch    channel
	opts  Options
	tty   *terminal
	det   *Detacher
	input <-chan []byte
	winch <-chan os.Signal
	ack   *atomic.Uint64
	w     *frameWriter

	rows uint16
	cols uint16

	reconnect bool

	lastPong  atomic.Int64
	received  atomic.Uint64
	gotFrame  atomic.Bool
	announced atomic.Bool
}

// runSession sends Hello, runs the reader and writer loops, and returns once both
// have stopped. The caller owns ctx cancellation and the channel.
func runSession(a *attach) sessionResult {
	hello := proto.Hello{
		Version:    version.ProtocolVersion,
		LastAckSeq: a.ack.Load(),
		Rows:       a.rows,
		Cols:       a.cols,
	}
	payload, err := hello.MarshalBinary()
	if err != nil {
		return sessionResult{kind: resultFatal, err: fmt.Errorf("tether: cannot encode hello: %w", err)}
	}
	if err := a.w.write(proto.Message{Type: proto.TypeHello, Payload: payload}); err != nil {
		return sessionResult{kind: resultReconnect, err: err}
	}
	// The size from Hello is applied before the first replay byte, but send a
	// Resize as well so a session created by an older client is corrected.
	if err := a.sendResize(a.rows, a.cols); err != nil {
		return sessionResult{kind: resultReconnect, err: err}
	}

	results := make(chan sessionResult, 2)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		results <- a.readLoop()
	}()
	go func() {
		defer wg.Done()
		results <- a.writeLoop(stop)
	}()

	first := <-results

	if first.kind == resultDetach {
		// Give the daemon a moment to close the channel; Detach must not be lost.
		select {
		case <-results:
		case <-time.After(detachGrace):
		}
		wg.Wait()
		a.ch.Close()
		first.attached = a.gotFrame.Load()
		return first
	}

	close(stop)
	// Unblock whichever loop is still parked in Read or Write.
	_ = a.ch.SetDeadline(time.Now())
	second := <-results
	wg.Wait()
	a.ch.Close()

	merged := mergeResults(first, second)
	merged.attached = a.gotFrame.Load()
	return merged
}

// mergeResults prefers the most specific outcome when both loops stop. A reader
// that saw Exit or Error outranks the writer's generic reconnect result.
func mergeResults(a, b sessionResult) sessionResult {
	rank := func(k resultKind) int {
		switch k {
		case resultReconnect:
			return 0
		case resultCancelled:
			return 1
		case resultDetach:
			return 2
		case resultExit:
			return 3
		case resultFatal:
			return 4
		default:
			return -1
		}
	}
	if rank(b.kind) > rank(a.kind) {
		return b
	}
	return a
}

// readLoop decodes frames and writes terminal bytes to stdout immediately, with
// no inspection or translation.
func (a *attach) readLoop() sessionResult {
	for {
		m, err := proto.ReadMessage(a.ch)
		if err != nil {
			return a.readFailure(err)
		}
		a.markAlive()
		a.gotFrame.Store(true)

		switch m.Type {
		case proto.TypeData:
			seq, p, err := proto.DataSeq(m)
			if err != nil {
				continue // malformed frame: ignore rather than tear down
			}
			if len(p) == 0 {
				continue
			}
			if a.reconnect && !a.announced.Swap(true) && isTTY(a.opts.Stderr) {
				fmt.Fprintf(a.opts.Stderr, "\r\ntether: reattached (replayed %d bytes)\r\n", len(p))
			}
			if _, err := a.opts.Stdout.Write(p); err != nil {
				return sessionResult{kind: resultReconnect, err: err}
			}
			a.ack.Store(lastAckFor(seq, len(p)))
			a.received.Add(uint64(len(p)))

		case proto.TypeOverrun:
			from, to, err := proto.OverrunRange(m)
			if err != nil || to <= from {
				continue
			}
			if _, err := fmt.Fprintf(a.opts.Stdout, "%s\r\n", ringbuf.NoticeOverrun); err != nil {
				return sessionResult{kind: resultReconnect, err: err}
			}

		case proto.TypeExit:
			code, err := proto.ExitCode(m)
			if err != nil {
				return sessionResult{kind: resultReconnect, err: err}
			}
			return sessionResult{kind: resultExit, code: code}

		case proto.TypePing:
			if err := a.w.write(proto.Message{Type: proto.TypePong}); err != nil {
				return sessionResult{kind: resultReconnect, err: err}
			}

		case proto.TypePong:
			// Liveness was recorded above; nothing else to do.

		case proto.TypeError:
			msg := strings.TrimSpace(string(m.Payload))
			if msg == "" {
				msg = "remote error"
			}
			return sessionResult{kind: resultFatal, err: fmt.Errorf("tether: %s", msg)}

		default:
			// Unknown frame types are ignored so the protocol stays
			// forward-compatible (docs/PROTOCOL.md §2).
		}
	}
}

// readFailure classifies an abandoned read. A channel that dies before the
// first frame with status 127 means the remote binary is not installed.
func (a *attach) readFailure(err error) sessionResult {
	if !a.gotFrame.Load() {
		if code, ok := exitStatus(a.ch); ok && code == 127 {
			return sessionResult{kind: resultFatal, err: &ErrRemoteMissing{Host: a.opts.Host}}
		}
	}
	return sessionResult{kind: resultReconnect, err: err}
}

// writeLoop forwards stdin, acks delivered bytes, serialises SIGWINCH into
// Resize frames, runs the heartbeat and watches the dead-peer deadline.
func (a *attach) writeLoop(stop <-chan struct{}) sessionResult {
	ackTicker := time.NewTicker(ackEvery)
	defer ackTicker.Stop()
	pingTicker := time.NewTicker(heartbeatEvery)
	defer pingTicker.Stop()
	// The liveness watchdog is separate from the ping ticker so the 9 s
	// dead-peer verdict lands within livenessEvery of the deadline, no matter
	// where the last ping fell.
	liveness := time.NewTicker(livenessEvery)
	defer liveness.Stop()

	sentAck := a.ack.Load()

	for {
		select {
		case <-a.ctx.Done():
			return sessionResult{kind: resultCancelled}

		case <-stop:
			return sessionResult{kind: resultReconnect}

		case chunk := <-a.input:
			out, detach := a.det.Filter(chunk)
			if detach {
				if err := a.w.write(proto.Message{Type: proto.TypeDetach}); err != nil {
					return sessionResult{kind: resultReconnect, err: err}
				}
				return sessionResult{kind: resultDetach}
			}
			for off := 0; off < len(out); off += inputChunk {
				end := off + inputChunk
				if end > len(out) {
					end = len(out)
				}
				if err := a.w.writeData(out[off:end]); err != nil {
					return sessionResult{kind: resultReconnect, err: err}
				}
			}

		case <-ackTicker.C:
			if cur := a.ack.Load(); cur != sentAck {
				payload := make([]byte, 8)
				binary.BigEndian.PutUint64(payload, cur)
				if err := a.w.write(proto.Message{Type: proto.TypeAck, Payload: payload}); err != nil {
					return sessionResult{kind: resultReconnect, err: err}
				}
				sentAck = cur
			}

		case <-pingTicker.C:
			if err := a.w.write(proto.Message{Type: proto.TypePing}); err != nil {
				return sessionResult{kind: resultReconnect, err: err}
			}

		case <-liveness.C:
			lastInbound := time.Unix(0, a.lastPong.Load())
			if !heartbeatExpired(lastInbound, time.Now()) {
				continue
			}
			// Force the parked reads and writes to fail; the reconnect loop
			// then runs exactly as it does for a broken connection.
			silent := time.Since(lastInbound)
			_ = a.ch.SetDeadline(time.Now())
			return sessionResult{
				kind: resultReconnect,
				err:  fmt.Errorf("no reply for %s", silent.Round(time.Second)),
			}

		case <-a.winch:
			rows, cols := a.tty.size()
			if rows == a.rows && cols == a.cols {
				continue
			}
			a.rows, a.cols = rows, cols
			if err := a.sendResize(rows, cols); err != nil {
				return sessionResult{kind: resultReconnect, err: err}
			}
		}
	}
}

func (a *attach) sendResize(rows, cols uint16) error {
	payload, err := proto.Resize{Rows: rows, Cols: cols}.MarshalBinary()
	if err != nil {
		return fmt.Errorf("tether: cannot encode resize: %w", err)
	}
	return a.w.write(proto.Message{Type: proto.TypeResize, Payload: payload})
}

func (a *attach) markAlive() {
	a.lastPong.Store(time.Now().UnixNano())
}

// heartbeatExpired reports whether the connection has been silent for longer
// than the dead-peer limit. Any inbound frame refreshes lastInbound, so the
// limit is measured from the last Pong or Data byte, per docs/PROTOCOL.md §6
// (three missed heartbeats at 3 s each).
func heartbeatExpired(lastInbound, now time.Time) bool {
	if lastInbound.IsZero() {
		return false
	}
	return now.Sub(lastInbound) > heartbeatLimit
}

// lastAckFor returns the highest sequence number covered by a Data frame that
// starts at firstSeq and carries n bytes. Empty frames acknowledge nothing, so
// they report the cursor just before firstSeq.
func lastAckFor(firstSeq uint64, n int) uint64 {
	if n <= 0 {
		if firstSeq == 0 {
			return 0
		}
		return firstSeq - 1
	}
	return firstSeq + uint64(n) - 1
}

// sleepCtx waits for d or for ctx to be cancelled, reporting which happened.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
