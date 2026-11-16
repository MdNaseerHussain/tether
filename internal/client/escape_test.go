package client

import (
	"strings"
	"testing"
)

func TestDetacherMatchAfterEnter(t *testing.T) {
	d, err := NewDetacher("~.")
	if err != nil {
		t.Fatalf("NewDetacher: %v", err)
	}

	out, detach := d.Filter([]byte("ls\r"))
	if detach {
		t.Fatal("detached before the sequence was typed")
	}
	if string(out) != "ls\r" {
		t.Fatalf("out = %q, want %q", out, "ls\r")
	}

	out, detach = d.Filter([]byte("~."))
	if !detach {
		t.Fatal("expected detach after Enter + ~.")
	}
	if len(out) != 0 {
		t.Fatalf("matched bytes leaked: %q", out)
	}
}

func TestDetacherNotArmedWithoutEnter(t *testing.T) {
	d, _ := NewDetacher("~.")

	out, detach := d.Filter([]byte("~."))
	if detach {
		t.Fatal("detached without a preceding Enter")
	}
	if string(out) != "~." {
		t.Fatalf("out = %q, want %q", out, "~.")
	}
}

func TestDetacherPartialMatchAcrossCalls(t *testing.T) {
	d, _ := NewDetacher("~.")

	out, detach := d.Filter([]byte("echo hi\n~"))
	if detach {
		t.Fatal("detached on a partial match")
	}
	if string(out) != "echo hi\n" {
		t.Fatalf("out = %q, want %q", out, "echo hi\n")
	}

	out, detach = d.Filter([]byte("."))
	if !detach {
		t.Fatal("expected detach when the sequence completes")
	}
	if len(out) != 0 {
		t.Fatalf("matched bytes leaked: %q", out)
	}
}

func TestDetacherMismatchFlushes(t *testing.T) {
	d, _ := NewDetacher("~.")

	if _, detach := d.Filter([]byte("\n")); detach {
		t.Fatal("unexpected detach")
	}
	out, detach := d.Filter([]byte("~a"))
	if detach {
		t.Fatal("mismatch must not detach")
	}
	if string(out) != "~a" {
		t.Fatalf("out = %q, want %q", out, "~a")
	}
}

func TestDetacherPreservesDoubleTilde(t *testing.T) {
	d, _ := NewDetacher("~.")

	if _, detach := d.Filter([]byte("x\n")); detach {
		t.Fatal("unexpected detach")
	}
	out, detach := d.Filter([]byte("~~"))
	if detach {
		t.Fatal("~~ must not detach")
	}
	if string(out) != "~~" {
		t.Fatalf("out = %q, want %q", out, "~~")
	}
}

func TestDetacherMismatchingEnterRearms(t *testing.T) {
	d, _ := NewDetacher("~.")

	if _, detach := d.Filter([]byte("\n")); detach {
		t.Fatal("unexpected detach")
	}
	// "~" starts a match, Enter is a mismatch; both pass through and the Enter
	// leaves the parser armed at a new line.
	out, detach := d.Filter([]byte("~\n"))
	if detach {
		t.Fatal("unexpected detach")
	}
	if string(out) != "~\n" {
		t.Fatalf("out = %q, want %q", out, "~\n")
	}
	if _, detach := d.Filter([]byte("~.")); !detach {
		t.Fatal("expected detach after the re-arming Enter")
	}
}

func TestDetacherEnterAlwaysPasses(t *testing.T) {
	d, _ := NewDetacher("^]")

	for _, enter := range []string{"\r", "\n"} {
		out, detach := d.Filter([]byte(enter))
		if detach {
			t.Fatalf("%q detached", enter)
		}
		if string(out) != enter {
			t.Fatalf("out = %q, want %q", out, enter)
		}
	}
}

func TestDetacherMultiByteSpec(t *testing.T) {
	d, _ := NewDetacher("~~.")
	if _, detach := d.Filter([]byte("hi\n")); detach {
		t.Fatal("unexpected detach")
	}
	if _, detach := d.Filter([]byte("~~")); detach {
		t.Fatal("detached on a partial multi-byte match")
	}
	if _, detach := d.Filter([]byte(".")); !detach {
		t.Fatal("expected detach on the final byte")
	}
}

func TestNewDetacherValidation(t *testing.T) {
	if _, err := NewDetacher(""); err == nil {
		t.Fatal("empty sequence must be rejected")
	}
	if _, err := NewDetacher(strings.Repeat("x", 9)); err == nil {
		t.Fatal("sequence longer than 8 bytes must be rejected")
	}
	if _, err := NewDetacher(strings.Repeat("x", 8)); err != nil {
		t.Fatalf("8-byte sequence rejected: %v", err)
	}

	d, err := NewDetacher("none")
	if err != nil {
		t.Fatalf("none: %v", err)
	}
	out, detach := d.Filter([]byte("\n~."))
	if detach {
		t.Fatal("disabled detacher must never detach")
	}
	if string(out) != "\n~." {
		t.Fatalf("disabled detacher changed input: %q", out)
	}
}
