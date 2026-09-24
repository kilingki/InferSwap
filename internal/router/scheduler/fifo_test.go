package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/runtime"
)

type grantRec struct {
	model string
	err   error
	serve bool
}

type fakeEffects struct {
	states  map[string]runtime.State
	status  map[string]runtime.Status
	grants  []grantRec
	unloads [][]string
	loads   []string
	serveOK bool
}

func newFake(states map[string]runtime.State) *fakeEffects {
	return &fakeEffects{
		states:  states,
		status:  map[string]runtime.Status{},
		serveOK: true,
	}
}

func (f *fakeEffects) ModelState(id string) (runtime.State, bool) {
	st, ok := f.states[id]
	return st, ok
}

func (f *fakeEffects) RunningModels() map[string]runtime.State {
	out := map[string]runtime.State{}
	for id, st := range f.states {
		if st != runtime.StateStopped && st != runtime.StateUnknown && st != runtime.StateShutdown {
			out[id] = st
		}
	}
	return out
}

func (f *fakeEffects) LastStatus(id string) (runtime.Status, bool) {
	st, ok := f.status[id]
	return st, ok
}

func (f *fakeEffects) StartUnload(ids []string) {
	f.unloads = append(f.unloads, append([]string{}, ids...))
}
func (f *fakeEffects) StartLoad(modelID string) { f.loads = append(f.loads, modelID) }

func (f *fakeEffects) GrantError(req HandlerReq, err error) {
	f.grants = append(f.grants, grantRec{model: req.Model, err: err})
}

func (f *fakeEffects) GrantServe(req HandlerReq, modelID string) bool {
	ok := f.serveOK
	f.grants = append(f.grants, grantRec{model: modelID, serve: ok})
	return ok
}

type exclusive struct{}

func (exclusive) EvictionFor(target string, running []string) []string {
	var out []string
	for _, id := range running {
		if id != target {
			out = append(out, id)
		}
	}
	return out
}

func testCfg() *config.Config {
	return &config.Config{
		MaxQueueSize: 8,
		Models: map[string]config.Model{
			"A": {ID: "A", ConcurrencyLimit: 1},
			"B": {ID: "B", ConcurrencyLimit: 1},
		},
	}
}

func req(id uint64, model string) HandlerReq {
	return HandlerReq{ID: id, Model: model, Ctx: context.Background()}
}

func newSched(f *fakeEffects) *FIFO {
	return NewFIFO(testCfg(), exclusive{}, f)
}

func TestReadyHeadGrantsImmediately(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	if len(f.grants) != 1 || !f.grants[0].serve {
		t.Fatalf("grants=%v", f.grants)
	}
	if len(f.loads) != 0 {
		t.Fatal("ready head should not load")
	}
}

func TestAInFlightThenBSwap(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	if len(f.unloads) != 0 {
		t.Fatal("must not unload while A busy")
	}
	s.OnServeDone(ServeDoneEvent{ModelID: "A"})
	f.status["A"] = runtime.Status{ActiveRequests: 0}
	s.OnStatus(StatusEvent{ModelID: "A", Status: f.status["A"]})
	if len(f.unloads) != 1 {
		t.Fatalf("unloads=%v", f.unloads)
	}
	f.states["A"] = runtime.StateStopped
	s.OnUnloadDone(UnloadDone{IDs: []string{"A"}})
	if len(f.loads) != 1 || f.loads[0] != "B" {
		t.Fatalf("loads=%v", f.loads)
	}
	f.states["B"] = runtime.StateReady
	s.OnSwapDone(SwapDone{ModelID: "B"})
	if len(f.grants) != 2 || f.grants[1].model != "B" {
		t.Fatalf("grants=%v", f.grants)
	}
}

func TestNoReadyFastPathOverEarlierWaiter(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	s.OnRequest(req(3, "A"))
	if len(f.grants) != 1 {
		t.Fatalf("A2 must not skip B1, grants=%v", f.grants)
	}
	s.OnServeDone(ServeDoneEvent{ModelID: "A"})
	f.status["A"] = runtime.Status{ActiveRequests: 0}
	s.OnStatus(StatusEvent{ModelID: "A", Status: f.status["A"]})
	f.states["A"] = runtime.StateStopped
	s.OnUnloadDone(UnloadDone{IDs: []string{"A"}})
	f.states["B"] = runtime.StateReady
	s.OnSwapDone(SwapDone{ModelID: "B"})
	if f.grants[1].model != "B" {
		t.Fatalf("expected B next, grants=%v", f.grants)
	}
}

func TestLoadShareDoesNotSkipFIFO(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateStopped, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	if len(f.loads) != 1 {
		t.Fatalf("loads=%v", f.loads)
	}
	s.OnRequest(req(2, "B"))
	s.OnRequest(req(3, "A"))
	f.states["A"] = runtime.StateReady
	s.OnSwapDone(SwapDone{ModelID: "A"})
	if len(f.grants) != 1 || f.grants[0].model != "A" {
		t.Fatalf("only A1 after load, grants=%v", f.grants)
	}
	if s.QueueLen() != 2 {
		t.Fatalf("B1 and A2 should remain queued, n=%d", s.QueueLen())
	}
}

func TestUnloadFailureBlocksLoad(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "B"))
	if len(f.unloads) != 1 {
		t.Fatal("expected unload A")
	}
	s.OnUnloadDone(UnloadDone{IDs: []string{"A"}, Err: errors.New("unload failed")})
	if len(f.loads) != 0 {
		t.Fatalf("load after unload fail: %v", f.loads)
	}
	if len(f.grants) != 1 || f.grants[0].err != ErrUnloadBlocked {
		t.Fatalf("grants=%v", f.grants)
	}
	s.OnRequest(req(2, "B"))
	if len(f.loads) != 0 {
		t.Fatal("hidden recovery load")
	}
}

func TestLoadFailureReturned(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateStopped, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnSwapDone(SwapDone{ModelID: "A", Err: errors.New("load failed")})
	if len(f.grants) != 1 || f.grants[0].err != ErrLoadFailed {
		t.Fatalf("grants=%v", f.grants)
	}
}

func TestCancelDoesNotStartLoad(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	s.OnCancel(req(2, "B"))
	s.OnServeDone(ServeDoneEvent{ModelID: "A"})
	f.status["A"] = runtime.Status{ActiveRequests: 0}
	s.OnStatus(StatusEvent{ModelID: "A", Status: f.status["A"]})
	if len(f.unloads) != 0 || len(f.loads) != 0 {
		t.Fatalf("cancel caused swap unloads=%v loads=%v", f.unloads, f.loads)
	}
}

func TestBNotStarvedByContinuousA(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	s.OnRequest(req(3, "A"))
	s.OnRequest(req(4, "A"))
	s.OnServeDone(ServeDoneEvent{ModelID: "A"})
	f.status["A"] = runtime.Status{ActiveRequests: 0}
	s.OnStatus(StatusEvent{ModelID: "A", Status: f.status["A"]})
	f.states["A"] = runtime.StateStopped
	s.OnUnloadDone(UnloadDone{IDs: []string{"A"}})
	f.states["B"] = runtime.StateReady
	s.OnSwapDone(SwapDone{ModelID: "B"})
	if f.grants[1].model != "B" {
		t.Fatalf("B starved, grants=%v", f.grants)
	}
}

func TestConcurrencyQueuesNot429(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "A"))
	for _, g := range f.grants {
		if errors.Is(g.err, ErrQueueFull) {
			t.Fatal("concurrency must not 429")
		}
	}
	if len(f.grants) != 1 {
		t.Fatalf("A2 should wait, grants=%v", f.grants)
	}
	if s.QueueLen() != 1 {
		t.Fatalf("queue=%d", s.QueueLen())
	}
}

func TestQueueFull429(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	cfg := testCfg()
	cfg.MaxQueueSize = 1
	s := NewFIFO(cfg, exclusive{}, f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	s.OnRequest(req(3, "B"))
	found := false
	for _, g := range f.grants {
		if errors.Is(g.err, ErrQueueFull) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 429, grants=%v", f.grants)
	}
}

func TestServeDoneBackendActiveBlocksUnload(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	s.OnStatus(StatusEvent{ModelID: "A", Status: runtime.Status{ActiveRequests: 1}})
	s.OnServeDone(ServeDoneEvent{ModelID: "A"})
	if len(f.unloads) != 0 {
		t.Fatal("unload while backend active")
	}
	s.OnBackendDone(BackendDoneEvent{ModelID: "A"})
	if len(f.unloads) != 1 {
		t.Fatalf("unloads after backend done=%v", f.unloads)
	}
}

func TestReopenGateWhenBCanceledBeforeUnload(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	if !s.GateClosed("A") {
		t.Fatal("A gate should close for B")
	}
	s.OnCancel(req(2, "B"))
	if s.GateClosed("A") {
		t.Fatal("A gate should reopen")
	}
	s.OnServeDone(ServeDoneEvent{ModelID: "A"})
	f.status["A"] = runtime.Status{ActiveRequests: 0}
	s.OnStatus(StatusEvent{ModelID: "A", Status: f.status["A"]})
	if len(f.unloads) != 0 {
		t.Fatal("unloaded after demand vanished")
	}
}

func TestEmptyQueueReopensGate(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: make(chan bool, 1)})
	s.OnRequest(req(2, "B"))
	if !s.GateClosed("A") {
		t.Fatal("expected A closed")
	}
	s.OnCancel(req(2, "B"))
	if s.GateClosed("A") {
		t.Fatal("empty queue left gate closed")
	}
	if len(f.unloads) != 0 {
		t.Fatal("unload with empty queue")
	}
}

func TestUnloadInProgressNoDuplicateLoad(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "B"))
	if !s.PhaseUnloading() {
		t.Fatal("expected unload")
	}
	s.OnCancel(req(1, "B"))
	s.OnRequest(req(2, "A"))
	s.OnUnloadDone(UnloadDone{IDs: []string{"A"}})
	f.states["A"] = runtime.StateStopped
	bLoads := 0
	for _, id := range f.loads {
		if id == "B" {
			bLoads++
		}
	}
	if bLoads != 0 {
		t.Fatalf("duplicate B load: %v", f.loads)
	}
}

func TestPendingToHTTPNoDoubleCount(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	if s.Pending("A") != 1 || s.HTTPIn("A") != 0 {
		t.Fatalf("pending=%d http=%d", s.Pending("A"), s.HTTPIn("A"))
	}
	ok := make(chan bool, 1)
	s.OnProxyStart(ProxyStartEvent{ModelID: "A", OK: ok})
	if !<-ok {
		t.Fatal("proxy start rejected")
	}
	if s.Pending("A") != 0 || s.HTTPIn("A") != 1 {
		t.Fatalf("after start pending=%d http=%d", s.Pending("A"), s.HTTPIn("A"))
	}
}

func TestCancelGrantedReleasesPending(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady, "B": runtime.StateStopped})
	s := newSched(f)
	s.OnRequest(req(1, "A"))
	s.OnCancelGranted(CancelGrantedEvent{ModelID: "A"})
	if s.Pending("A") != 0 {
		t.Fatalf("pending=%d", s.Pending("A"))
	}
}

func TestUnknownModel(t *testing.T) {
	f := newFake(map[string]runtime.State{"A": runtime.StateReady})
	s := newSched(f)
	s.OnRequest(req(1, "nope"))
	if len(f.grants) != 1 || f.grants[0].err != ErrModelNotFound {
		t.Fatalf("grants=%v", f.grants)
	}
}
