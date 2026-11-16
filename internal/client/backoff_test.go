package client

import (
	"math/rand"
	"testing"
	"time"
)

func TestBackoffGrowth(t *testing.T) {
	b := NewBackoff(500*time.Millisecond, 5*time.Second, rand.New(rand.NewSource(1)))

	var prev time.Duration
	for i := 0; i < 4; i++ {
		d := b.Next()
		if d < 500*time.Millisecond || d > 5*time.Second {
			t.Fatalf("delay %s outside [500ms, 5s]", d)
		}
		if i > 0 && d <= prev {
			t.Fatalf("delay did not grow: %s after %s", d, prev)
		}
		prev = d
	}
}

func TestBackoffCap(t *testing.T) {
	b := NewBackoff(500*time.Millisecond, 5*time.Second, rand.New(rand.NewSource(2)))

	for i := 0; i < 50; i++ {
		d := b.Next()
		if d > 5*time.Second {
			t.Fatalf("delay %s exceeds the cap", d)
		}
		if d < 500*time.Millisecond {
			t.Fatalf("delay %s is below the initial floor", d)
		}
		// Once the nominal delay reaches the cap every jittered value stays in
		// the top band (5s - 20%).
		if i == 49 && d < 4*time.Second {
			t.Fatalf("final delay = %s, want a value near the 5s cap", d)
		}
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	b := NewBackoff(time.Second, 10*time.Second, rand.New(rand.NewSource(3)))

	seen := map[time.Duration]bool{}
	for i := 0; i < 500; i++ {
		b.Reset()
		d := b.Next()
		// Nominal is 1s and jitter is +/-20%, clamped to the initial floor.
		if d < time.Second || d > 1200*time.Millisecond {
			t.Fatalf("delay %s outside [1s, 1.2s]", d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatalf("jitter produced no variation: %v", seen)
	}
}

func TestBackoffReset(t *testing.T) {
	b := NewBackoff(500*time.Millisecond, 5*time.Second, rand.New(rand.NewSource(4)))

	for i := 0; i < 6; i++ {
		b.Next()
	}
	b.Reset()

	d := b.Next()
	if d < 500*time.Millisecond || d > 600*time.Millisecond {
		t.Fatalf("delay after Reset = %s, want the initial 500ms band", d)
	}
}

func TestBackoffNilRand(t *testing.T) {
	b := NewBackoff(10*time.Millisecond, 20*time.Millisecond, nil)
	for i := 0; i < 10; i++ {
		if d := b.Next(); d < 10*time.Millisecond || d > 20*time.Millisecond {
			t.Fatalf("delay %s outside bounds", d)
		}
	}
}
