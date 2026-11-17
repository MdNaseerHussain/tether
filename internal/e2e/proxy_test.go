//go:build !windows

package e2e

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// gate is the switch that makes a proxied connection lossy. Passing is the
// default (a closed channel); blackholing swaps in an open channel that every
// copy loop blocks on. Both loops wait before each Read *and* before each
// Write, so a blackholed link consumes nothing: bytes stay in the kernel
// buffers, exactly as they would on a path that stopped forwarding.
type gate struct {
	mu      sync.Mutex
	ch      chan struct{}
	blocked bool
}

func newGate() *gate {
	ch := make(chan struct{})
	close(ch)
	return &gate{ch: ch}
}

// wait blocks while the gate is blackholed and returns immediately otherwise.
func (g *gate) wait() {
	g.mu.Lock()
	ch := g.ch
	g.mu.Unlock()
	<-ch
}

// blackhole stops all forwarding until pass is called.
func (g *gate) blackhole() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.blocked {
		return
	}
	g.ch = make(chan struct{})
	g.blocked = true
}

// pass resumes forwarding.
func (g *gate) pass() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.blocked {
		return
	}
	close(g.ch)
	g.blocked = false
}

// proxyConn is one client connection and the upstream connection it is paired
// with. Closing either side drops both, which is what a broken pipe does.
type proxyConn struct {
	client net.Conn
	server net.Conn
	once   sync.Once
}

func (pc *proxyConn) close() {
	pc.once.Do(func() {
		_ = pc.client.Close()
		_ = pc.server.Close()
	})
}

// proxy forwards TCP between the tether client and the SSH server, with a gate
// that can blackhole the link and an operation that can drop every connection
// while keeping the listener open for reconnects.
type proxy struct {
	ln     net.Listener
	target string
	gate   *gate

	mu       sync.Mutex
	conns    map[*proxyConn]struct{}
	accepted int
	accepts  []time.Time
	closed   bool

	wg sync.WaitGroup
}

func newProxy(target string) (*proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("proxy listen: %w", err)
	}
	p := &proxy{
		ln:     ln,
		target: target,
		gate:   newGate(),
		conns:  make(map[*proxyConn]struct{}),
	}
	p.wg.Add(1)
	go p.acceptLoop()
	return p, nil
}

// Port is the loopback port the client dials.
func (p *proxy) Port() int {
	return p.ln.Addr().(*net.TCPAddr).Port
}

func (p *proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.DialTimeout("tcp", p.target, 3*time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = client.Close()
			_ = server.Close()
			continue
		}
		pc := &proxyConn{client: client, server: server}
		p.conns[pc] = struct{}{}
		p.accepted++
		p.accepts = append(p.accepts, time.Now())
		p.mu.Unlock()

		p.wg.Add(2)
		go p.copyLoop(pc, server, client) // client -> server
		go p.copyLoop(pc, client, server) // server -> client
	}
}

// copyLoop moves bytes in one direction, honouring the gate on both the read
// and the write side. A chunk read just before a blackhole is held in memory
// and delivered when the gate opens again; nothing is discarded.
func (p *proxy) copyLoop(pc *proxyConn, dst, src net.Conn) {
	defer p.wg.Done()
	defer func() {
		pc.close()
		p.mu.Lock()
		delete(p.conns, pc)
		p.mu.Unlock()
	}()

	buf := make([]byte, 32<<10)
	for {
		p.gate.wait()
		n, err := src.Read(buf)
		if n > 0 {
			p.gate.wait()
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// Accepted is the number of TCP connections the proxy has taken from the
// client. It counts a connection as soon as the client's SYN is answered, so it
// is not evidence that the SSH handshake completed.
func (p *proxy) Accepted() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

// AcceptTimes returns the time each connection was accepted, in order. The
// timestamp is taken when the client's TCP connection is answered, which is the
// moment the client gave up on the old transport and started a new one.
func (p *proxy) AcceptTimes() []time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]time.Time(nil), p.accepts...)
}

// WaitAccepted blocks until n connections have been accepted or timeout
// expires.
func (p *proxy) WaitAccepted(n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if got := p.Accepted(); got >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("proxy accepted %d connection(s), want %d within %s", p.Accepted(), n, timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Blackhole makes the link stop forwarding in both directions.
func (p *proxy) Blackhole() { p.gate.blackhole() }

// Pass restores forwarding.
func (p *proxy) Pass() { p.gate.pass() }

// DropAll closes both ends of every live proxied connection, which the peers
// see as a reset/broken pipe. The listener stays open so the client can
// reconnect immediately.
func (p *proxy) DropAll() int {
	p.mu.Lock()
	conns := make([]*proxyConn, 0, len(p.conns))
	for pc := range p.conns {
		conns = append(conns, pc)
	}
	p.conns = make(map[*proxyConn]struct{})
	p.mu.Unlock()

	for _, pc := range conns {
		pc.close()
	}
	return len(conns)
}

// Close stops the listener and every connection. The gate is opened first so a
// blackholed copy loop cannot block shutdown forever.
func (p *proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	conns := make([]*proxyConn, 0, len(p.conns))
	for pc := range p.conns {
		conns = append(conns, pc)
	}
	p.mu.Unlock()

	p.gate.pass()
	_ = p.ln.Close()
	for _, pc := range conns {
		pc.close()
	}
	p.wg.Wait()
}
