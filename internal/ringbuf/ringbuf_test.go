package ringbuf

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestEmptyBuffer(t *testing.T) {
	b := New(16)

	if got := b.Head(); got != 0 {
		t.Fatalf("Head() = %d, want 0", got)
	}
	if got := b.OldestSeq(); got != 1 {
		t.Fatalf("OldestSeq() = %d, want 1", got)
	}
	if got := b.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}
	if got := b.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d, want 0", got)
	}
	if got := b.Capacity(); got != 16 {
		t.Fatalf("Capacity() = %d, want 16", got)
	}

	data, start, overrun := b.ReadFrom(0)
	if len(data) != 0 || start != 1 || overrun {
		t.Fatalf("ReadFrom(0) = (%q, %d, %v), want (empty, 1, false)", data, start, overrun)
	}
}

func TestNewDefaultsCapacity(t *testing.T) {
	if got := New(0).Capacity(); got != DefaultCapacity {
		t.Fatalf("New(0).Capacity() = %d, want %d", got, DefaultCapacity)
	}
	if got := New(-5).Capacity(); got != DefaultCapacity {
		t.Fatalf("New(-5).Capacity() = %d, want %d", got, DefaultCapacity)
	}
}

func TestWriteAndReadFromZero(t *testing.T) {
	b := New(64)
	n, err := b.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write = (%d, %v), want (5, nil)", n, err)
	}

	data, start, overrun := b.ReadFrom(0)
	if overrun {
		t.Fatal("ReadFrom(0) reported overrun on a fresh buffer")
	}
	if start != 1 {
		t.Fatalf("startSeq = %d, want 1", start)
	}
	if string(data) != "hello" {
		t.Fatalf("data = %q, want %q", data, "hello")
	}

	// A zero-length write must not disturb the sequence space.
	if n, err := b.Write(nil); n != 0 || err != nil {
		t.Fatalf("Write(nil) = (%d, %v), want (0, nil)", n, err)
	}
	if got := b.Head(); got != 5 {
		t.Fatalf("Head() = %d, want 5", got)
	}
}

func TestSequenceNumbering(t *testing.T) {
	b := New(64)
	b.Write([]byte("abc"))
	b.Write([]byte("de"))

	if got := b.Head(); got != 5 {
		t.Fatalf("Head() = %d, want 5", got)
	}
	if got := b.OldestSeq(); got != 1 {
		t.Fatalf("OldestSeq() = %d, want 1", got)
	}
	if got := b.Size(); got != 5 {
		t.Fatalf("Size() = %d, want 5", got)
	}

	// Replay after the fifth byte is empty: the cursor is at the newest byte.
	data, start, overrun := b.ReadFrom(5)
	if len(data) != 0 || start != 6 || overrun {
		t.Fatalf("ReadFrom(5) = (%q, %d, %v), want (empty, 6, false)", data, start, overrun)
	}

	// Replay after the third byte yields exactly the bytes that follow it, and
	// data[0] carries sequence number 4.
	data, start, overrun = b.ReadFrom(3)
	if string(data) != "de" || start != 4 || overrun {
		t.Fatalf("ReadFrom(3) = (%q, %d, %v), want (\"de\", 4, false)", data, start, overrun)
	}

	// Sequence numbers of individual bytes: seq(n) lives at offset (n-1)%cap.
	// Cursor 0 is the "start of retained data" special case, so the first byte
	// is checked through the full replay.
	for i, want := range []string{"a", "b", "c", "d", "e"} {
		seq := uint64(i + 1)
		data, start, _ := b.ReadFrom(seq - 1)
		if seq == 1 {
			if start != 1 || string(data) != "abcde" {
				t.Fatalf("ReadFrom(0) = (%q, %d), want (\"abcde\", 1)", data, start)
			}
			continue
		}
		if start != seq || string(data[0]) != want {
			t.Fatalf("ReadFrom(%d) = (%q, %d), want first byte %q at seq %d", seq-1, data, start, want, seq)
		}
		if rest := string(data); rest != "abcde"[i:] {
			t.Fatalf("ReadFrom(%d) = %q, want %q", seq-1, rest, "abcde"[i:])
		}
	}
}

func TestWraparoundReplay(t *testing.T) {
	b := New(8)
	b.Write([]byte("abcd"))
	b.Write([]byte("efghij")) // wraps: only the last 8 bytes remain

	if got := b.Head(); got != 10 {
		t.Fatalf("Head() = %d, want 10", got)
	}
	if got := b.Size(); got != 8 {
		t.Fatalf("Size() = %d, want 8", got)
	}
	if got := b.OldestSeq(); got != 3 {
		t.Fatalf("OldestSeq() = %d, want 3", got)
	}
	if got := b.Dropped(); got != 2 {
		t.Fatalf("Dropped() = %d, want 2", got)
	}

	data, start, overrun := b.ReadFrom(0)
	if overrun {
		t.Fatal("ReadFrom(0) must not report overrun when it starts at the oldest byte")
	}
	if start != 3 {
		t.Fatalf("startSeq = %d, want 3", start)
	}
	if string(data) != "cdefghij" {
		t.Fatalf("data = %q, want %q", data, "cdefghij")
	}

	// A cursor inside the retained window replays only the remainder.
	data, start, overrun = b.ReadFrom(8)
	if string(data) != "ij" || start != 9 || overrun {
		t.Fatalf("ReadFrom(8) = (%q, %d, %v), want (\"ij\", 9, false)", data, start, overrun)
	}
}

func TestOverrunWhenCursorPredatesOldest(t *testing.T) {
	b := New(8)
	b.Write([]byte("abcdefghij")) // fixed window: seq 3..10

	data, start, overrun := b.ReadFrom(1)
	if !overrun {
		t.Fatal("ReadFrom(1) must report overrun: seq 2 was overwritten")
	}
	if start != 3 {
		t.Fatalf("startSeq = %d, want 3 (clamped to the retained window)", start)
	}
	if string(data) != "cdefghij" {
		t.Fatalf("data = %q, want %q", data, "cdefghij")
	}
	if len(data) != b.Size() {
		t.Fatalf("len(data) = %d, want Size() = %d", len(data), b.Size())
	}
}

func TestWriteLargerThanCapacity(t *testing.T) {
	b := New(4)
	n, err := b.Write([]byte("abcdefgh"))
	if n != 8 || err != nil {
		t.Fatalf("Write = (%d, %v), want (8, nil)", n, err)
	}
	if got := b.Head(); got != 8 {
		t.Fatalf("Head() = %d, want 8", got)
	}
	if got := b.Size(); got != 4 {
		t.Fatalf("Size() = %d, want 4", got)
	}
	if got := b.OldestSeq(); got != 5 {
		t.Fatalf("OldestSeq() = %d, want 5", got)
	}
	if got := b.Dropped(); got != 4 {
		t.Fatalf("Dropped() = %d, want 4", got)
	}

	data, start, overrun := b.ReadFrom(0)
	if string(data) != "efgh" || start != 5 || overrun {
		t.Fatalf("ReadFrom(0) = (%q, %d, %v), want (\"efgh\", 5, false)", data, start, overrun)
	}
}

func TestReadFromHeadIsEmpty(t *testing.T) {
	b := New(32)
	b.Write([]byte("streaming"))

	head := b.Head()
	data, start, overrun := b.ReadFrom(head)
	if len(data) != 0 {
		t.Fatalf("ReadFrom(Head) = %q, want empty", data)
	}
	if start != head+1 {
		t.Fatalf("startSeq = %d, want %d", start, head+1)
	}
	if overrun {
		t.Fatal("ReadFrom(Head) must not report overrun")
	}
}

func TestReadFromBeyondHead(t *testing.T) {
	b := New(32)
	b.Write([]byte("abc"))

	head := b.Head()
	data, start, overrun := b.ReadFrom(head + 1)
	if !overrun {
		t.Fatal("ReadFrom(Head+1) must report overrun")
	}
	if len(data) != 0 {
		t.Fatalf("ReadFrom(Head+1) = %q, want empty", data)
	}
	if start != head+1 {
		t.Fatalf("startSeq = %d, want %d", start, head+1)
	}
}

func TestReadFromReturnsCopy(t *testing.T) {
	b := New(4)
	b.Write([]byte("abcd"))

	data, _, _ := b.ReadFrom(0)
	if len(data) != 4 {
		t.Fatalf("len(data) = %d, want 4", len(data))
	}
	// Mutating the returned slice must not corrupt the buffer.
	data[0] = 'X'
	again, _, _ := b.ReadFrom(0)
	if string(again) != "abcd" {
		t.Fatalf("buffer was aliased: %q", again)
	}
}

func TestDroppedAccounting(t *testing.T) {
	b := New(8)
	if got := b.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d, want 0", got)
	}

	b.Write([]byte("12345678"))
	if got := b.Dropped(); got != 0 {
		t.Fatalf("Dropped() after exact fit = %d, want 0", got)
	}

	b.Write([]byte("9"))
	if got := b.Dropped(); got != 1 {
		t.Fatalf("Dropped() = %d, want 1", got)
	}

	b.Write([]byte("abcdefghij"))
	if got := b.Dropped(); got != 11 {
		t.Fatalf("Dropped() = %d, want 11", got)
	}

	// Dropped is cumulative and never shrinks as data wraps around.
	if got := b.Dropped(); got != b.Head()-uint64(b.Capacity()) {
		t.Fatalf("Dropped() = %d, want Head-Capacity = %d", got, b.Head()-uint64(b.Capacity()))
	}
}

func TestReset(t *testing.T) {
	b := New(8)
	b.Write([]byte("abcdefghij"))
	b.Reset()

	if got := b.Head(); got != 0 {
		t.Fatalf("Head() = %d, want 0", got)
	}
	if got := b.OldestSeq(); got != 1 {
		t.Fatalf("OldestSeq() = %d, want 1", got)
	}
	if got := b.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}
	if got := b.Dropped(); got != 0 {
		t.Fatalf("Dropped() = %d, want 0", got)
	}
	if data, _, _ := b.ReadFrom(0); len(data) != 0 {
		t.Fatalf("ReadFrom(0) after Reset = %q, want empty", data)
	}

	b.Write([]byte("z"))
	if data, start, _ := b.ReadFrom(0); string(data) != "z" || start != 1 {
		t.Fatalf("after Reset+Write, ReadFrom(0) = (%q, %d), want (\"z\", 1)", data, start)
	}
}

func TestConcurrentWritersAndReaders(t *testing.T) {
	const (
		writers       = 8
		perWriter     = 500
		chunk         = 17
		bufferCapSize = 1024
	)
	b := New(bufferCapSize)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := bytes.Repeat([]byte{byte('A' + id)}, chunk)
			for j := 0; j < perWriter; j++ {
				n, err := b.Write(payload)
				if err != nil || n != chunk {
					t.Errorf("Write = (%d, %v), want (%d, nil)", n, err, chunk)
					return
				}
			}
		}(i)
	}

	// Readers hammer the buffer while the writers run; -race turns any unsafe
	// sharing into a failure.
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				data, start, _ := b.ReadFrom(0)
				if len(data) > 0 && start == 0 {
					t.Errorf("non-empty replay with startSeq 0")
					return
				}
				_ = b.Size()
				_ = b.Head()
				_ = b.OldestSeq()
				_ = b.Dropped()
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	wantHead := uint64(writers * perWriter * chunk)
	if got := b.Head(); got != wantHead {
		t.Fatalf("Head() = %d, want %d", got, wantHead)
	}
	if got := b.Size(); got != bufferCapSize {
		t.Fatalf("Size() = %d, want %d", got, bufferCapSize)
	}
	if got := b.Dropped(); got != wantHead-uint64(bufferCapSize) {
		t.Fatalf("Dropped() = %d, want %d", got, wantHead-uint64(bufferCapSize))
	}

	// The retained window must be internally consistent: the number of bytes
	// replayed equals the distance between the oldest and newest sequence.
	data, start, overrun := b.ReadFrom(0)
	if overrun {
		t.Fatal("ReadFrom(0) must not overrun")
	}
	if start != b.OldestSeq() {
		t.Fatalf("startSeq = %d, want OldestSeq() = %d", start, b.OldestSeq())
	}
	if uint64(len(data)) != b.Head()-start+1 {
		t.Fatalf("len(data) = %d, want %d", len(data), b.Head()-start+1)
	}
}

func TestNoticeOverrunText(t *testing.T) {
	if NoticeOverrun == "" {
		t.Fatal("NoticeOverrun must not be empty")
	}
	if fmt.Sprintf("%s", NoticeOverrun) != "[tether: output buffer overrun; display truncated]" {
		t.Fatalf("NoticeOverrun = %q", NoticeOverrun)
	}
}
