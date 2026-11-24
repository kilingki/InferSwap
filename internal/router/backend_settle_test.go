package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/runtime"
)

type settlingStatusRT struct {
	*stubRT
	calls     atomic.Int32
	failFirst bool
	observed  chan struct{}
}

func (s *settlingStatusRT) Status(ctx context.Context) (runtime.Status, error) {
	n := s.calls.Add(1)
	st, err := s.stubRT.Status(ctx)
	if n == 1 {
		close(s.observed)
		if s.failFirst {
			return runtime.Status{}, errors.New("temporary status failure")
		}
	}
	return st, err
}

// HTTP completion precedes asynchronous backend cleanup. Without another request,
// the quiet generation must keep observing until it can release its slot.
func TestRouterRechecksBackendAfterHTTPCompletion(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		name := "active_backend"
		if failFirst {
			name = "temporary_status_error"
		}
		t.Run(name, func(t *testing.T) {
			a := &settlingStatusRT{stubRT: &stubRT{
				state:  runtime.StateReady,
				status: runtime.Status{ActiveRequests: 1},
			}, failFirst: failFirst, observed: make(chan struct{})}
			r := NewExclusive(testRouterCfg(), map[string]runtime.Runtime{"A": a}, nil)
			defer r.shutdownFn()
			w := httptest.NewRecorder()
			r.ServeModel("A", w, httptest.NewRequest(http.MethodPost, "/infer", nil))
			if w.Code != http.StatusOK {
				t.Fatalf("HTTP %d", w.Code)
			}
			select {
			case <-a.observed:
			case <-time.After(time.Second):
				t.Fatal("initial status was not observed")
			}
			a.mu.Lock()
			a.status.ActiveRequests = 0
			a.mu.Unlock()
			deadline := time.Now().Add(2 * time.Second)
			for {
				protected := true
				r.onLoop(func() { protected = r.schedule.Protected("A") })
				if !protected {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("quiet backend slot remained protected after cleanup; status calls=%d", a.calls.Load())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if a.calls.Load() < 2 {
				t.Fatal("backend status was not rechecked")
			}
			calls := a.calls.Load()
			time.Sleep(400 * time.Millisecond)
			if a.calls.Load() != calls {
				t.Fatal("status polling continued after backend became idle")
			}
		})
	}
}
