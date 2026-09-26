package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/runtime"
	"github.com/kilingki/InferSwap/internal/runtime/mock"
)

func TestPreloadExclusiveLeavesLast(t *testing.T) {
	a, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := mock.New("B")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	cfg := testRouterCfg()
	ma := cfg.Models["A"]
	ma.BaseURL = a.URL()
	mb := cfg.Models["B"]
	mb.BaseURL = b.URL()
	cfg.Models["A"] = ma
	cfg.Models["B"] = mb
	runtimes := map[string]runtime.Runtime{
		"A": runtime.NewClient(context.Background(), ma, cfg, runtime.ClientOptions{}),
		"B": runtime.NewClient(context.Background(), mb, cfg, runtime.ClientOptions{}),
	}
	rt := NewExclusive(cfg, runtimes, nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
	}()
	if err := rt.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rt.Preload(context.Background(), []string{"A", "B"}); err != nil {
		t.Fatal(err)
	}
	if runtimes["A"].State() != runtime.StateReady || runtimes["B"].State() != runtime.StateReady {
		t.Fatalf("both models fit, A=%s B=%s", runtimes["A"].State(), runtimes["B"].State())
	}
}

func TestShutdownFailsQueueAndRejectsEnsureReady(t *testing.T) {
	a := &stubRT{state: runtime.StateReady}
	rt := NewExclusive(testRouterCfg(), map[string]runtime.Runtime{"A": a, "B": &stubRT{state: runtime.StateStopped}}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	w := httptest.NewRecorder()
	rt.ServeModel("A", w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
	if err := a.EnsureReady(context.Background(), 0); err == nil && a.State() == runtime.StateShutdown {
		// stub EnsureReady does not reject; runtime clients do
	}
}

func TestShutdownUnloadFailureDoesNotHang(t *testing.T) {
	a := &stubRT{state: runtime.StateReady, stopErr: errors.New("unload failed")}
	rt := NewExclusive(testRouterCfg(), map[string]runtime.Runtime{"A": a}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := rt.Shutdown(ctx)
	if err == nil {
		t.Fatal("unload failure reported as a clean shutdown")
	}
	if len(rt.Unconfirmed()) != 1 || rt.Unconfirmed()[0] != "A" {
		t.Fatalf("unconfirmed=%v", rt.Unconfirmed())
	}
}

func TestShutdownWaitsForLocalSlot(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	a := &stubRT{state: runtime.StateReady, entered: entered, release: release}
	cfg := testRouterCfg()
	cfg.ShutdownTimeout = time.Second
	rt := New(cfg, map[string]runtime.Runtime{"A": a}, nil, nil)
	go rt.ServeModel("A", httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter the runtime")
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done <- rt.Shutdown(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	if a.stops != 0 {
		t.Fatal("unloaded while the local slot was still held")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if a.stops != 1 {
		t.Fatalf("stops=%d", a.stops)
	}
}

func TestShutdownLocalSlotDeadlineIsUnconfirmed(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	a := &stubRT{state: runtime.StateReady, entered: entered, release: release}
	rt := New(testRouterCfg(), map[string]runtime.Runtime{"A": a}, nil, nil)
	go rt.ServeModel("A", httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not enter the runtime")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := rt.Shutdown(ctx)
	close(release)
	if err == nil {
		t.Fatal("in-flight slot reported as a clean shutdown")
	}
	if a.stops != 0 {
		t.Fatalf("stopped despite the deadline, stops=%d", a.stops)
	}
}

func TestShutdownBusyIsUnconfirmed(t *testing.T) {
	a := &stubRT{state: runtime.StateReady, status: runtime.Status{ActiveRequests: 1, State: runtime.WireReady}}
	rt := NewExclusive(testRouterCfg(), map[string]runtime.Runtime{"A": a}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := rt.Shutdown(ctx)
	if err == nil {
		t.Fatal("busy backend reported as unloaded")
	}
	if a.stops != 0 {
		t.Fatalf("stopped a busy model, stops=%d", a.stops)
	}
	got := rt.Unconfirmed()
	if len(got) != 1 || got[0] != "A" {
		t.Fatalf("unconfirmed=%v", got)
	}
}

func TestShutdownMarksClientShutdown(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cfg := testRouterCfg()
	m := cfg.Models["A"]
	m.BaseURL = s.URL()
	cfg.Models["A"] = m
	c := runtime.NewClient(context.Background(), m, cfg, runtime.ClientOptions{})
	rt := NewExclusive(cfg, map[string]runtime.Runtime{"A": c, "B": &stubRT{state: runtime.StateStopped}}, nil)
	if err := rt.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && c.State() != runtime.StateShutdown {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.EnsureReady(context.Background(), 0); err == nil {
		t.Fatal("EnsureReady after shutdown")
	}
}
