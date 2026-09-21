package testkit

import (
	"context"
	"testing"
	"time"
)

func TestBarrierReleaseAfterHit(t *testing.T) {
	b := NewBarrier()
	done := make(chan error, 1)
	go func() {
		done <- b.Hit(context.Background())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("Hit returned before Release: %v", err)
	default:
	}
	b.Release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Hit did not return after Release")
	}
}

func TestBarrierReleaseBeforeHit(t *testing.T) {
	b := NewBarrier()
	b.Release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Hit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBarrierHitCanceled(t *testing.T) {
	b := NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Hit(ctx); err == nil {
		t.Fatal("expected context error")
	}
}
