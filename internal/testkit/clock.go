package testkit

import (
	"sync"
	"time"
)

// Clock is a time source tests can replace with a fake.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// RealClock uses the process wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
	fired    bool
}

// FakeClock is a controllable clock. Advance fires due After waiters
// without sleeping.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func NewFakeClock(now time.Time) *FakeClock {
	if now.IsZero() {
		now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &FakeClock{now: now}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	t := &fakeTimer{deadline: c.now.Add(d), ch: ch}
	c.timers = append(c.timers, t)
	return ch
}

// Advance moves the clock forward and delivers due timers.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due []chan time.Time
	var keep []*fakeTimer
	for _, t := range c.timers {
		if t.fired {
			continue
		}
		if !t.deadline.After(now) {
			t.fired = true
			due = append(due, t.ch)
			continue
		}
		keep = append(keep, t)
	}
	c.timers = keep
	c.mu.Unlock()
	for _, ch := range due {
		ch <- now
	}
}
