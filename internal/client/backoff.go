package client

import (
	"math/rand"
	"time"
)

// Backoff produces the jittered exponential delays used by the reconnect loop.
//
// The first Next returns initial, and every subsequent nominal delay doubles up
// to max. Each returned value is jittered by +/-20% and clamped to
// [initial, max], so the client retries promptly after a blip but never hammers
// an unreachable host.
type Backoff struct {
	initial time.Duration
	max     time.Duration
	rnd     *rand.Rand
	next    time.Duration
}

// NewBackoff returns a Backoff that starts at initial and doubles to max. A nil
// rnd uses a time-seeded source.
func NewBackoff(initial, max time.Duration, rnd *rand.Rand) *Backoff {
	if initial <= 0 {
		initial = time.Millisecond
	}
	if max < initial {
		max = initial
	}
	if rnd == nil {
		rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Backoff{initial: initial, max: max, rnd: rnd, next: initial}
}

// Next returns the next delay and advances the sequence.
func (b *Backoff) Next() time.Duration {
	base := b.next
	if base < b.initial {
		base = b.initial
	}
	if base > b.max {
		base = b.max
	}

	d := time.Duration(float64(base) * (0.8 + 0.4*b.rnd.Float64()))
	if d < b.initial {
		d = b.initial
	}
	if d > b.max {
		d = b.max
	}

	if b.next < b.max {
		b.next *= 2
		if b.next > b.max {
			b.next = b.max
		}
	}
	return d
}

// Reset returns the sequence to its initial delay, which the reconnect loop does
// after an attach succeeds so a later outage retries quickly again.
func (b *Backoff) Reset() {
	b.next = b.initial
}
