package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/runtime"
)

type stubRT struct {
	mu      sync.Mutex
	state   runtime.State
	stopErr error
	ensures int
	stops   int
	status  runtime.Status
	entered chan struct{}
	release chan struct{}
}

func (s *stubRT) Reconcile(ctx context.Context) error { return nil }
func (s *stubRT) Shutdown()                           {}

func (s *stubRT) EnsureReady(ctx context.Context, timeout time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensures++
	s.state = runtime.StateReady
	return nil
}

func (s *stubRT) Stop(ctx context.Context, timeout time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stops++
	if s.stopErr != nil {
		return s.stopErr
	}
	s.state = runtime.StateStopped
	return nil
}

func (s *stubRT) Status(ctx context.Context) (runtime.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status, nil
}

func (s *stubRT) State() runtime.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *stubRT) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.entered != nil {
		select {
		case <-s.entered:
		default:
			close(s.entered)
		}
	}
	if s.release != nil {
		<-s.release
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, string(s.State()))
}

func testRouterCfg() *config.Config {
	return &config.Config{
		MaxQueueSize:  16,
		QueueTimeout:  time.Minute,
		UnloadTimeout: time.Second,
		Models: map[string]config.Model{
			"A": {ID: "A", ConcurrencyLimit: 1, UnloadTimeout: time.Second, HealthCheckTimeout: time.Second},
			"B": {ID: "B", ConcurrencyLimit: 1, UnloadTimeout: time.Second, HealthCheckTimeout: time.Second},
		},
	}
}

func TestExclusiveEviction(t *testing.T) {
	ev := exclusiveSwapper{}.EvictionFor("B", []string{"A", "B"})
	if len(ev) != 1 || ev[0] != "A" {
		t.Fatalf("evict=%v", ev)
	}
}

func TestRouterServeReadyModel(t *testing.T) {
	a := &stubRT{state: runtime.StateReady}
	b := &stubRT{state: runtime.StateStopped}
	rt := NewExclusive(testRouterCfg(), map[string]runtime.Runtime{"A": a, "B": b}, nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
	}()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	rt.ServeModel("A", w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
}

func TestRouterUnknownModel(t *testing.T) {
	a := &stubRT{state: runtime.StateReady}
	rt := NewExclusive(testRouterCfg(), map[string]runtime.Runtime{"A": a}, nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
	}()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	rt.ServeModel("missing", w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
}
