package client

import (
	"testing"
	"time"
)

// TestHeartbeatBudget pins the documented budget: three missed heartbeats at
// 3 s each are nine seconds, and the watchdog has to evaluate that deadline far
// more often than it pings, otherwise the verdict is quantised to the ping
// ticker and lands late.
func TestHeartbeatBudget(t *testing.T) {
	if heartbeatEvery != 3*time.Second {
		t.Fatalf("heartbeatEvery = %s, want 3s", heartbeatEvery)
	}
	if heartbeatLimit != 3*heartbeatEvery {
		t.Fatalf("heartbeatLimit = %s, want 3 missed heartbeats (%s)", heartbeatLimit, 3*heartbeatEvery)
	}
	if livenessEvery <= 0 || livenessEvery > time.Second {
		t.Fatalf("livenessEvery = %s, want a sub-second watchdog period", livenessEvery)
	}
	// Detection error is bounded by one watchdog period on top of the 9 s
	// budget; it must stay well under the 3 s ping cadence.
	if livenessEvery >= heartbeatEvery {
		t.Fatalf("livenessEvery = %s must be smaller than heartbeatEvery = %s", livenessEvery, heartbeatEvery)
	}
}

// TestHeartbeatExpired exercises the watchdog decision at its boundaries. The
// predicate is pure, so no real time has to pass.
func TestHeartbeatExpired(t *testing.T) {
	base := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name   string
		silent time.Duration
		want   bool
	}{
		{"fresh frame", 0, false},
		{"one missed heartbeat", heartbeatEvery, false},
		{"two missed heartbeats", 2 * heartbeatEvery, false},
		{"just under the limit", heartbeatLimit - time.Millisecond, false},
		{"exactly at the limit", heartbeatLimit, false},
		{"just over the limit", heartbeatLimit + time.Millisecond, true},
		{"watchdog period past the limit", heartbeatLimit + livenessEvery, true},
		{"far past the limit", 30 * time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := heartbeatExpired(base, base.Add(tc.silent)); got != tc.want {
				t.Fatalf("heartbeatExpired(silent=%s) = %v, want %v", tc.silent, got, tc.want)
			}
		})
	}

	// A zero timestamp means liveness was never recorded (a caller bug); the
	// watchdog must not tear the connection down for it.
	if heartbeatExpired(time.Time{}, base) {
		t.Fatal("heartbeatExpired(zero, now) = true, want false")
	}

	// Detection is not tied to the ping phase: a frame that arrived mid-tick and
	// then silence triggers exactly at the limit, not at the next ping.
	arrived := base.Add(1234 * time.Millisecond)
	if heartbeatExpired(arrived, arrived.Add(9*time.Second)) {
		t.Fatal("expired at exactly 9s from the last inbound frame")
	}
	if !heartbeatExpired(arrived, arrived.Add(9*time.Second+time.Millisecond)) {
		t.Fatal("not expired just past 9s from the last inbound frame")
	}
}
