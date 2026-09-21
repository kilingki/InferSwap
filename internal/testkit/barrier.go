package testkit

import (
	"context"
	"sync"
)

// Barrier is a one-shot start/release gate. Tests wait until Hit is
// observed, then Release to let the waiter continue. Release may run
// first; Hit then returns immediately.
type Barrier struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
	relOnce   sync.Once
}

func NewBarrier() *Barrier {
	return &Barrier{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

// Hit records that the operation reached the gate and blocks until Release.
func (b *Barrier) Hit(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.startOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitStarted blocks until Hit has been called.
func (b *Barrier) WaitStarted(ctx context.Context) error {
	if b == nil {
		return nil
	}
	select {
	case <-b.started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release unblocks Hit. Idempotent.
func (b *Barrier) Release() {
	if b == nil {
		return
	}
	b.relOnce.Do(func() { close(b.release) })
}

// Started reports whether Hit has been observed.
func (b *Barrier) Started() bool {
	if b == nil {
		return false
	}
	select {
	case <-b.started:
		return true
	default:
		return false
	}
}
