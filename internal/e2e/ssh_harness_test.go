//go:build !windows

package e2e

// These tests drive the real `tether` client and the real `tetherd` daemon over
// a real SSH connection that the harness can make lossy. Nothing is stubbed
// above the socket layer: the client authenticates with a generated key,
// verifies the host through known_hosts, runs `tetherd attach --spawn` over
// SSH exec, and speaks the framed protocol to a supervisor that really owns a
// PTY.

import (
	"bytes"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
)

// streamScript is the "remote workload" for the replay and heartbeat tests. It
// emits a numbered line every 5 ms and then holds the PTY open, which is what a
// long-running chat or build would do.
const streamScript = `#!/bin/sh
i=0
while [ $i -lt 400 ]; do
	i=$((i+1))
	printf 'LINE %05d\n' "$i"
	sleep 0.005
done
sleep 600
`

// sizeScript reports the PTY geometry continuously, so a resize is visible as a
// change in the output.
const sizeScript = `#!/bin/sh
while :; do
	stty size
	sleep 0.15
done
`

// osc52Script writes one clipboard escape and then exits, so the test can assert
// both byte-exact passthrough and a clean client exit.
const osc52Script = `#!/bin/sh
printf '\033]52;c;dGV0aGVyLWNsaXA=\007'
sleep 1
exit 0
`

// TestSSHMidStreamDropReplays drops the TCP link mid-stream and requires the
// client to reconnect to the surviving supervisor and to reconstruct the byte
// stream exactly: every line once, in order.
func TestSSHMidStreamDropReplays(t *testing.T) {
	f := newSSHFixture(t, streamScript, "drop")
	c := f.startClient(t)

	first := eventually(t, 20*time.Second, "LINE 00020 on the client's stdout", func() bool {
		return bytes.Contains(c.stdout.Bytes(), []byte("LINE 00020"))
	}, func() string { return f.dumpClient(c) })

	before := f.sessionInfo(t)
	if !before.Alive {
		t.Fatalf("session is not alive before the drop: %+v", before)
	}
	if f.srv.AuthCount() == 0 {
		t.Fatalf("the client never authenticated; the harness is not exercising SSH auth\n%s", f.dumpClient(c))
	}

	dropAt := time.Now()
	if dropped := f.prx.DropAll(); dropped == 0 {
		t.Fatalf("DropAll closed no connections\n%s", f.dumpClient(c))
	}

	reconnect := eventually(t, 15*time.Second, "a second successful exec after the drop", func() bool {
		return f.srv.ExecCount() >= 2
	}, func() string { return f.dumpClient(c) })
	t.Logf("drop: second exec on the remote host after %s", reconnect.Round(time.Millisecond))
	if elapsed := time.Since(dropAt); elapsed > 5*time.Second {
		t.Errorf("reconnect took %s; a broken pipe should retry in about 500ms", elapsed)
	}

	// Ack semantics: the replay must resume exactly after the last byte the
	// client acknowledged, so the concatenated stdout is the original stream.
	replayed := eventually(t, 25*time.Second, "all 400 lines exactly once and in order", func() bool {
		next, problems := scanStream(c.stdout.Bytes())
		return len(problems) == 0 && next == 401
	}, func() string { return f.dumpClient(c) })
	t.Logf("drop: LINE 00020 after %s; the other 380 lines arrived in %s after the drop",
		first.Round(time.Millisecond), replayed.Round(time.Millisecond))

	if next, problems := scanStream(c.stdout.Bytes()); next != 401 || len(problems) != 0 {
		t.Fatalf("stream is not exactly-once: consumed %d lines, problems %v", next-1, problems)
	}

	after := f.sessionInfo(t)
	if after.PID != before.PID {
		t.Fatalf("the supervisor was replaced by the drop: pid %d -> %d", before.PID, after.PID)
	}
	if !after.Alive {
		t.Fatalf("session is not alive after the reconnect: %+v", after)
	}
	t.Logf("drop: supervisor pid %d survived the reconnect", after.PID)

	f.stop(t)
}

// TestSSHBlackholeTriggersHeartbeatTimeout stops forwarding without closing the
// TCP connection. The client cannot learn about the dead path from the socket,
// so only the 3 s ping / 9 s dead-peer timer can tear it down. The lower bound
// on the reconnect delay is what proves that.
func TestSSHBlackholeTriggersHeartbeatTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("the blackhole test holds a dead link open for the heartbeat window")
	}
	f := newSSHFixture(t, streamScript, "blackhole")
	c := f.startClient(t)

	eventually(t, 20*time.Second, "LINE 00020 on the client's stdout", func() bool {
		return bytes.Contains(c.stdout.Bytes(), []byte("LINE 00020"))
	}, func() string { return f.dumpClient(c) })

	t0 := time.Now()
	f.prx.Blackhole()

	// Nothing must reconnect while the path is dead: no TCP error, no reset.
	<-time.After(blackholeWindow)
	if got := f.srv.ExecCount(); got != 1 {
		t.Fatalf("client reconnected while the link was blackholed (%d execs); the tear-down did not come from the heartbeat\n%s",
			got, f.dumpClient(c))
	}

	f.prx.Pass()

	eventually(t, 20*time.Second, "a reconnect after the link heals", func() bool {
		return f.srv.ExecCount() >= 2
	}, func() string { return f.dumpClient(c) })
	elapsed := time.Since(t0)
	t.Logf("blackhole: second exec %s after the link went dead (%s after the heal)",
		elapsed.Round(time.Millisecond), (elapsed - blackholeWindow).Round(time.Millisecond))

	// A TCP-level failure would reconnect in well under a second. Anything
	// above 8.5 s can only come from the 9 s dead-peer limit (a tick may fire
	// slightly after it). The exec itself arriving within milliseconds of the
	// heal also proves the client had already given up and was parked in a
	// stalled dial: a client that survived the blackhole would have kept its
	// connection and never started a second remote command.
	if elapsed < 8500*time.Millisecond {
		t.Fatalf("reconnect after %s is too fast to be the 9s heartbeat timeout", elapsed)
	}
	// Three 3 s ping ticks plus one backoff after a stalled handshake is the
	// worst case; 22 s leaves room for scheduling without hiding a regression.
	if elapsed > 22*time.Second {
		t.Fatalf("reconnect after %s exceeds the heartbeat budget", elapsed)
	}

	// The supervisor kept buffering while the client was unreachable, so the
	// replay has to be complete and duplicate-free.
	eventually(t, 20*time.Second, "all 400 lines exactly once and in order after the heal", func() bool {
		next, problems := scanStream(c.stdout.Bytes())
		return len(problems) == 0 && next == 401
	}, func() string { return f.dumpClient(c) })

	f.stop(t)
}

// TestSSHSIGWINCHResizeForwarded proves the whole resize path: a local
// TIOCSWINSZ plus SIGWINCH becomes a Resize frame, and the daemon applies it to
// the remote PTY with pty.Setsize.
func TestSSHSIGWINCHResizeForwarded(t *testing.T) {
	f := newSSHFixture(t, sizeScript, "resize")
	p := f.startPTYClient(t, 30, 90)

	eventually(t, 20*time.Second, "the initial 30x90 geometry on the remote PTY", func() bool {
		return bytes.Contains(p.output(), []byte("30 90"))
	}, func() string { return f.dumpPTY(p) })

	if err := pty.Setsize(p.master, &pty.Winsize{Rows: 40, Cols: 100}); err != nil {
		t.Fatalf("resize the local pty: %v", err)
	}
	if err := syscall.Kill(p.cmd.Process.Pid, syscall.SIGWINCH); err != nil {
		t.Fatalf("send SIGWINCH: %v", err)
	}

	eventually(t, 5*time.Second, "40x100 on the remote PTY after SIGWINCH", func() bool {
		return bytes.Contains(p.output(), []byte("40 100"))
	}, func() string { return f.dumpPTY(p) })

	snapshot := p.output()
	resizedAt := bytes.Index(snapshot, []byte("40 100"))
	atResize := bytes.Count(snapshot, []byte("40 100"))

	// Require further samples at the new size, so the pass is not one lucky
	// line straddling the resize.
	eventually(t, 5*time.Second, "further 40x100 samples", func() bool {
		return bytes.Count(p.output(), []byte("40 100")) >= atResize+3
	}, func() string { return f.dumpPTY(p) })

	if tail := p.output()[resizedAt:]; bytes.Contains(tail, []byte("30 90")) {
		t.Fatalf("the remote PTY reverted to its old size after the resize\n%s", f.dumpPTY(p))
	}

	f.stop(t)
}

// TestSSHOSC52Passthrough sends a clipboard escape through the whole stack and
// requires the exact bytes to arrive unchanged: no quoting, no stripping and no
// translation of the payload between ESC and BEL.
func TestSSHOSC52Passthrough(t *testing.T) {
	const marker = "\x1b]52;c;dGV0aGVyLWNsaXA=\x07" // base64("tether-clip")

	f := newSSHFixture(t, osc52Script, "osc52")
	c := f.startClient(t)

	eventually(t, 10*time.Second, "the OSC 52 sequence on stdout", func() bool {
		return bytes.Contains(c.stdout.Bytes(), []byte(marker))
	}, func() string { return f.dumpClient(c) })

	// Every OSC 52 introducer must be followed by exactly the workload's
	// payload and BEL, so a translated or re-quoted sequence is caught even if
	// the exact marker also appears elsewhere.
	data := c.stdout.Bytes()
	for off := 0; ; {
		i := bytes.Index(data[off:], []byte("\x1b]52;"))
		if i < 0 {
			break
		}
		start := off + i
		end := bytes.IndexByte(data[start:], 0x07)
		if end < 0 {
			t.Fatalf("unterminated OSC sequence at offset %d: %q", start, data[start:])
		}
		seq := data[start : start+end+1]
		if string(seq) != marker {
			t.Fatalf("OSC 52 bytes were modified in transit:\n got %q\nwant %q", seq, marker)
		}
		off = start + end + 1
	}

	code := c.waitCode(t, 10*time.Second)
	if code != 0 {
		t.Fatalf("client exited with %d, want 0 when the remote shell exits cleanly\n%s", code, f.dumpClient(c))
	}
	if c.stderr.Len() != 0 {
		t.Errorf("client wrote to stderr on a clean run: %q", c.stderr.Bytes())
	}

	f.stop(t)
}
