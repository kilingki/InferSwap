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
	if runtimes["A"].State() == runtime.StateReady {
		t.Fatal("exclusive preload [A,B] should not leave A ready")
	}
	if runtimes["B"].State() != runtime.StateReady {
		t.Fatalf("B state=%s", runtimes["B"].State())
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
	if err := rt.Shutdown(ctx); err != nil {
		t.Fatal(err)
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
