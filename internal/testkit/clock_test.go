package testkit

import (
	"testing"
	"time"
)

func TestFakeClockAfterFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(time.Time{})
	ch := c.After(time.Second)

	select {
	case <-ch:
		t.Fatal("timer fired before Advance")
	default:
	}

	c.Advance(500 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("timer fired too early")
	default:
	}

	c.Advance(500 * time.Millisecond)
	select {
	case got := <-ch:
		if got != c.Now() {
			t.Fatalf("got %v want %v", got, c.Now())
		}
	case <-time.After(time.Second):
		t.Fatal("timer did not fire after Advance")
	}
}

func TestFakeClockZeroDurationReady(t *testing.T) {
	c := NewFakeClock(time.Time{})
	select {
	case <-c.After(0):
	default:
		t.Fatal("zero duration After should be ready")
	}
}
