// Package ringbuf implements the sequence-numbered circular buffer that decouples
// the remote PTY from a possibly slow or disconnected client.
//
// Every byte written to a Buffer is assigned a strictly increasing sequence
// number starting at 1. Clients remember the sequence number of the last byte
// they rendered and ask the buffer to replay everything that follows it, so a
// reconnect after an arbitrary outage resumes exactly where the terminal left
// off instead of redrawing the screen.
package ringbuf

import "sync"

// DefaultCapacity is the number of bytes retained per session (4 MiB).
const DefaultCapacity = 4 << 20

// NoticeOverrun is emitted by the daemon when the client asked for data that has
// already been overwritten, so the terminal can explain the gap instead of
// silently splicing unrelated output together.
const NoticeOverrun = "[tether: output buffer overrun; display truncated]"

// Buffer is a fixed-capacity circular byte buffer with 1-based sequence
// numbers. It is safe for concurrent use: the PTY pump writes while attach
// handlers read.
type Buffer struct {
	mu       sync.Mutex
	buf      []byte
	capacity int
	head     uint64 // total bytes ever written == seq of the newest byte
	write    int    // index of the next byte to overwrite
}

// New returns a Buffer retaining at most capacity bytes. A non-positive capacity
// falls back to DefaultCapacity.
func New(capacity int) *Buffer {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Buffer{
		buf:      make([]byte, capacity),
		capacity: capacity,
	}
}

// Write appends p to the buffer, overwriting the oldest bytes once it is full.
// It never blocks and always reports len(p) bytes consumed, so a PTY pump can
// keep draining the child process while no client is attached.
func (b *Buffer) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Only the tail can survive when a single write exceeds the capacity.
	data := p
	if len(data) > b.capacity {
		data = data[len(data)-b.capacity:]
	}

	b.head += uint64(n)
	off := int((b.head - uint64(len(data))) % uint64(b.capacity))
	copied := copy(b.buf[off:], data)
	copy(b.buf, data[copied:])
	b.write = int(b.head % uint64(b.capacity))

	return n, nil
}

// Head returns the total number of bytes ever written, which is also the
// sequence number of the newest byte. It is 0 when nothing has been written.
func (b *Buffer) Head() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.head
}

// OldestSeq returns the sequence number of the oldest retained byte. It is 1
// while nothing has been dropped.
func (b *Buffer) OldestSeq() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.oldestLocked()
}

// Size returns the number of bytes currently retained.
func (b *Buffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.head < uint64(b.capacity) {
		return int(b.head)
	}
	return b.capacity
}

// Capacity returns the buffer capacity in bytes.
func (b *Buffer) Capacity() int { return b.capacity }

// Dropped returns the number of bytes overwritten because the writer outran the
// retention window.
func (b *Buffer) Dropped() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.head <= uint64(b.capacity) {
		return 0
	}
	return b.head - uint64(b.capacity)
}

// ReadFrom replays the retained bytes that follow from, where from is the last
// sequence number the peer has already consumed (0 when it has consumed
// nothing). startSeq is the sequence number of data[0]; overrun reports that
// the requested position had already been overwritten (or that from lies beyond
// Head) and the replay was clamped to the retained window.
//
// The returned slice is a copy and stays valid after the buffer is unlocked.
func (b *Buffer) ReadFrom(from uint64) (data []byte, startSeq uint64, overrun bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.head == 0 {
		return nil, 1, false
	}

	oldest := b.oldestLocked()
	start := from + 1 // zero means "immediately after from"
	if from == 0 {
		start = oldest
	}

	if start > b.head {
		// Cursor at the newest byte: nothing new to replay. A cursor beyond
		// Head claims bytes that were never written.
		return nil, b.head + 1, from > b.head
	}
	if start < oldest {
		overrun = true
		start = oldest
	}

	n := int(b.head - start + 1)
	data = make([]byte, n)
	off := int((start - 1) % uint64(b.capacity))
	copied := copy(data, b.buf[off:])
	copy(data[copied:], b.buf)
	return data, start, overrun
}

// Reset discards all retained data and restarts sequence numbering at 1.
func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.head = 0
	b.write = 0
	clear(b.buf)
}

// oldestLocked reports the oldest retained sequence number. Callers must hold mu.
func (b *Buffer) oldestLocked() uint64 {
	if b.head <= uint64(b.capacity) {
		return 1
	}
	return b.head - uint64(b.capacity) + 1
}
