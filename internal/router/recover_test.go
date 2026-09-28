package router

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/resource"
	"github.com/kilingki/InferSwap/internal/runtime"
	"github.com/kilingki/InferSwap/internal/runtime/mock"
	"github.com/kilingki/InferSwap/internal/testkit"
)

type gpuBox struct {
	mu   sync.Mutex
	snap resource.Snapshot
	err  error
	live bool
}

func (g *gpuBox) Observe() (resource.Snapshot, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return resource.Snapshot{}, g.err
	}
	s := g.snap
	if g.live || s.ObservedAt.IsZero() {
		s.ObservedAt = time.Now()
	}
	s.OK = true
	return s, nil
}

func (g *gpuBox) Set(s resource.Snapshot, live bool) {
	g.mu.Lock()
	g.snap = s
	g.err = nil
	g.live = live
	g.mu.Unlock()
}

func (g *gpuBox) Fail(err error) {
	g.mu.Lock()
	g.err = err
	g.mu.Unlock()
}

type prepCounter struct {
	mu sync.Mutex
	n  int
	fn func(context.Context) error
}

func (p *prepCounter) Run(ctx context.Context, argv []string, cwd string) error {
	p.mu.Lock()
	p.n++
	fn := p.fn
	p.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

func (p *prepCounter) N() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

func prof(peak, residual int64) config.ResourceProfile {
	return config.ResourceProfile{LoadPeakBytes: peak, InferencePeakBytes: peak, UnloadedResidualBytes: residual}
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func serve(rt *Router, model string) (int, string, http.Header) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	rt.ServeModel(model, w, req)
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body["code"], w.Header()
}

func serveCtx(rt *Router, model string, ctx context.Context) (int, string) {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	rt.ServeModel(model, w, req)
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body["code"]
}

type pair struct {
	t      *testing.T
	rt     *Router
	gpu    *gpuBox
	mocks  map[string]*mock.Server
	client map[string]*runtime.Client
	prep   *prepCounter
	cfg    *config.Config
}

func newPair(t *testing.T, names []string, total, free int64, opts map[string][]mock.Option) *pair {
	t.Helper()
	p := &pair{
		t:      t,
		mocks:  map[string]*mock.Server{},
		client: map[string]*runtime.Client{},
		prep:   &prepCounter{},
		gpu: &gpuBox{snap: resource.Snapshot{DeviceID: "fake", Total: uint64(total), Free: uint64(free)}, live: true},
		cfg: &config.Config{
			MaxQueueSize:       8,
			QueueTimeout:       3 * time.Second,
			UnloadTimeout:      time.Second,
			DrainTimeout:       3 * time.Second,
			StatusTimeout:      2 * time.Second,
			ShutdownTimeout:    time.Second,
			GPU:                config.GPU{MaxObservationAge: 5 * time.Second},
			Models:             map[string]config.Model{},
		},
	}
	p.prep.fn = func(ctx context.Context) error {
		if a := p.mocks["A"]; a != nil {
			return a.Prepare(ctx)
		}
		return nil
	}
	rts := map[string]runtime.Runtime{}
	for _, name := range names {
		s, err := mock.New(name, opts[name]...)
		if err != nil {
			t.Fatal(err)
		}
		p.mocks[name] = s
		m := config.Model{
			ID:                 name,
			BaseURL:            s.URL(),
			ConcurrencyLimit:   1,
			HealthCheckTimeout: 2 * time.Second,
			UnloadTimeout:      time.Second,
			PrepareTimeout:     2 * time.Second,
			ResourceProfile:    prof(20, 1),
			Prepare:            &config.Prepare{Argv: []string{"/abs/prepare-" + name}},
		}
		p.cfg.Models[name] = m
		c := runtime.NewClient(context.Background(), m, p.cfg, runtime.ClientOptions{Commander: p.prep})
		p.client[name] = c
		rts[name] = c
	}
	p.rt = New(p.cfg, rts, nil, p.gpu)
	t.Cleanup(func() {
		if !p.rt.shutting.Load() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = p.rt.Shutdown(ctx)
		}
		for _, s := range p.mocks {
			s.Close()
		}
	})
	return p
}

func (p *pair) reconcile() {
	p.t.Helper()
	if err := p.rt.Reconcile(context.Background()); err != nil {
		p.t.Fatal(err)
	}
}

func (p *pair) peak(id string) int64 {
	return p.cfg.Models[id].LoadBound()
}

func (p *pair) reserved(id string) int64 {
	var n int64
	p.rt.onLoop(func() { n = p.rt.reserved[id] })
	return n
}

func (p *pair) marked(id string) bool {
	var ok bool
	p.rt.onLoop(func() { _, ok = p.rt.residualAfter[id] })
	return ok
}

func (p *pair) releaseSamples() {
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: p.gpu.snap.Total, Free: p.gpu.snap.Free}, true)
	snap, err := p.gpu.Observe()
	if err != nil {
		return
	}
	select {
	case p.rt.observeCh <- snap:
	default:
	}
}

func TestFailedLocalReadyIdleServesWithoutLoad(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyResident)
	p.reconcile()
	p.mocks["A"].ForceReady()
	loads := p.mocks["A"].Counts().LoadStarts
	code, errCode, _ := serve(p.rt, "A")
	if code != 200 || errCode != "" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if p.mocks["A"].Counts().LoadStarts != loads {
		t.Fatalf("load restarted: %d", p.mocks["A"].Counts().LoadStarts)
	}
	if p.reserved("A") != p.peak("A") {
		t.Fatalf("reserved=%d", p.reserved("A"))
	}
	if p.client["A"].State() != runtime.StateReady {
		t.Fatalf("state=%s", p.client["A"].State())
	}
}

func TestEndpointDownPrepareOnlyAfterOtherBounds(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 80, map[string][]mock.Option{
		"A": {mock.WithEndpointDown()},
	})
	p.cfg.Models["A"] = withPeak(p.cfg.Models["A"], 60, 5)
	p.cfg.Models["B"] = withPeak(p.cfg.Models["B"], 60, 5)
	p.mocks["B"].ForceReady()
	p.reconcile()
	if p.client["A"].State() != runtime.StateUnknown || p.client["B"].State() != runtime.StateReady {
		t.Fatalf("A=%s B=%s", p.client["A"].State(), p.client["B"].State())
	}
	code, errCode, _ := serve(p.rt, "A")
	if code != 200 {
		t.Fatalf("code=%d err=%s prep=%d", code, errCode, p.prep.N())
	}
	if p.prep.N() == 0 || p.mocks["A"].Counts().LoadStarts == 0 || p.mocks["B"].Counts().UnloadStarts == 0 {
		t.Fatalf("prep=%d load=%d unload=%d", p.prep.N(), p.mocks["A"].Counts().LoadStarts, p.mocks["B"].Counts().UnloadStarts)
	}
}

func TestBothEndpointsDownDoNotPrepare(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, map[string][]mock.Option{
		"A": {mock.WithEndpointDown()},
		"B": {mock.WithEndpointDown()},
	})
	p.reconcile()
	code, errCode, _ := serve(p.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if p.prep.N() != 0 || p.mocks["A"].Counts().LoadStarts != 0 || p.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatalf("prep=%d Aload=%d Bload=%d", p.prep.N(), p.mocks["A"].Counts().LoadStarts, p.mocks["B"].Counts().LoadStarts)
	}
}

func TestStatusTimeoutHTTPAndBrokenJSONDoNotPrepare(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, map[string][]mock.Option{
		"A": {mock.WithEndpointDown()},
	})
	p.reconcile()
	p.mocks["A"].SetEndpointAlive(true)
	p.mocks["A"].HoldStatus()
	p.cfg.StatusTimeout = 200 * time.Millisecond
	code, errCode, _ := serve(p.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" || p.prep.N() != 0 {
		t.Fatalf("timeout code=%d err=%s prep=%d", code, errCode, p.prep.N())
	}
	p.mocks["A"].ReleaseStatus()

	p.mocks["A"].SetStatusMode(mock.StatusUnreliable)
	code, errCode, _ = serve(p.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" || p.prep.N() != 0 {
		t.Fatalf("http code=%d err=%s prep=%d", code, errCode, p.prep.N())
	}
	p.mocks["A"].SetStatusMode(mock.StatusMalformed)
	code, errCode, _ = serve(p.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" || p.prep.N() != 0 || p.mocks["A"].Counts().LoadStarts != 0 {
		t.Fatalf("json code=%d err=%s prep=%d", code, errCode, p.prep.N())
	}
}

func TestUnknownUnloadedSetsPeakUntilLaterSample(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, map[string][]mock.Option{
		"A": {mock.WithEndpointDown()},
	})
	p.reconcile()
	if p.reserved("A") != 0 || p.marked("A") {
		t.Fatal("boot reconcile reserved an unknown model")
	}
	p.mocks["A"].SetEndpointAlive(true)
	p.mocks["A"].ForceUnloaded()
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	done := make(chan struct{})
	go func() {
		code, errCode, _ := serve(p.rt, "A")
		if code != 200 {
			t.Errorf("code=%d err=%s", code, errCode)
		}
		close(done)
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.marked("A") })
	if p.reserved("A") != p.peak("A") {
		t.Fatalf("peak=%d", p.reserved("A"))
	}
	select {
	case <-done:
		t.Fatal("admitted before a post-status sample")
	case <-time.After(200 * time.Millisecond):
	}
	p.releaseSamples()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("load did not follow the sample")
	}
	if p.mocks["A"].Counts().LoadStarts != 1 || p.marked("A") {
		t.Fatalf("loads=%d marked=%v", p.mocks["A"].Counts().LoadStarts, p.marked("A"))
	}
}

func TestBootUnloadedReconcileKeepsNoReservation(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.reconcile()
	if p.client["A"].State() != runtime.StateStopped || p.client["A"].StateReason() != "reconcile" {
		t.Fatalf("state=%s reason=%s", p.client["A"].State(), p.client["A"].StateReason())
	}
	if p.reserved("A") != 0 || p.marked("A") {
		t.Fatal("boot unloaded path reserved memory")
	}
}

func TestReadyInferenceIgnoresOtherRecovery(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyResident)
	p.mocks["B"].ForceReady()
	p.reconcile()
	p.mocks["A"].HoldStatus()
	p.rt.onLoop(func() { p.rt.residualAfter["A"] = time.Now().Add(time.Hour) })
	base := p.mocks["A"].Counts().StatusGets
	code, _, _ := serve(p.rt, "B")
	if code != 200 {
		t.Fatalf("code=%d", code)
	}
	if p.mocks["A"].Counts().StatusGets != base {
		t.Fatal("ready inference waited on the other status")
	}
	p.mocks["A"].ReleaseStatus()
}

func TestTargetReadySkipsOtherProbeFailure(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyResident)
	p.mocks["B"].ForceFailed(mock.ResidencyResident)
	p.reconcile()
	p.mocks["A"].ForceReady()
	p.mocks["B"].SetStatusMode(mock.StatusMalformed)
	baseB := p.mocks["B"].Counts().StatusGets
	code, _, _ := serve(p.rt, "A")
	if code != 200 || p.mocks["A"].Counts().LoadStarts != 0 {
		t.Fatalf("code=%d loads=%d", code, p.mocks["A"].Counts().LoadStarts)
	}
	if p.mocks["B"].Counts().StatusGets != baseB {
		t.Fatal("recovered target waited for the other model")
	}
}

func TestReadyBusyThenLaterIdle(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.mocks["A"].SetWire(mock.StateReady, mock.ResidencyResident, 2)
	code, errCode, hdr := serve(p.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "LIFECYCLE_PENDING" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if hdr.Get("Retry-After") != "" {
		t.Fatal("lifecycle pending set Retry-After")
	}
	if p.client["A"].State() != runtime.StateFailed || p.reserved("A") != p.peak("A") || p.mocks["A"].Counts().LoadStarts != 0 {
		t.Fatalf("state=%s reserved=%d", p.client["A"].State(), p.reserved("A"))
	}
	p.mocks["A"].SetWire(mock.StateReady, mock.ResidencyResident, 0)
	code, _, _ = serve(p.rt, "A")
	if code != 200 || p.mocks["A"].Counts().LoadStarts != 0 {
		t.Fatalf("later code=%d loads=%d", code, p.mocks["A"].Counts().LoadStarts)
	}
}

func TestLoadTimeoutThenLoadingThenReadyWithoutSecondLoad(t *testing.T) {
	gate := testkit.NewBarrier()
	p := newPair(t, []string{"A"}, 100, 100, map[string][]mock.Option{
		"A": {mock.WithLoadGate(gate)},
	})
	m := p.cfg.Models["A"]
	m.HealthCheckTimeout = 300 * time.Millisecond
	p.cfg.Models["A"] = m
	p.reconcile()
	code, errCode, _ := serve(p.rt, "A")
	if code != http.StatusBadGateway || errCode != "LOAD_FAILED" {
		t.Fatalf("code=%d err=%s state=%s", code, errCode, p.client["A"].State())
	}
	if p.client["A"].State() != runtime.StateFailed || p.reserved("A") != p.peak("A") {
		t.Fatalf("state=%s reserved=%d", p.client["A"].State(), p.reserved("A"))
	}
	var serial string
	p.rt.onLoop(func() { serial = p.rt.loadSerial })
	if serial != "" {
		t.Fatalf("loadSerial=%q", serial)
	}
	p.mocks["A"].SetWire(mock.StateLoading, mock.ResidencyUnknown, 0)
	code, errCode, _ = serve(p.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "LIFECYCLE_PENDING" {
		t.Fatalf("loading code=%d err=%s", code, errCode)
	}
	if p.client["A"].State() == runtime.StateStarting || p.client["A"].State() == runtime.StateStopping {
		t.Fatalf("stuck in %s", p.client["A"].State())
	}
	gate.Release()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["A"].Snapshot().State == mock.StateReady })
	loads := p.mocks["A"].Counts().LoadStarts
	code, _, _ = serve(p.rt, "A")
	if code != 200 || p.mocks["A"].Counts().LoadStarts != loads {
		t.Fatalf("ready code=%d loads=%d want %d", code, p.mocks["A"].Counts().LoadStarts, loads)
	}
}

func TestInProgressProbeDoesNotStickLocalLifecycle(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyResident)
	p.reconcile()
	p.mocks["A"].SetWire(mock.StateUnloading, mock.ResidencyResident, 0)
	code, errCode, _ := serve(p.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "LIFECYCLE_PENDING" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if p.client["A"].State() != runtime.StateFailed {
		t.Fatalf("state=%s", p.client["A"].State())
	}
	p.mocks["A"].ForceReady()
	code, _, _ = serve(p.rt, "A")
	if code != 200 || p.mocks["A"].Counts().LoadStarts != 0 {
		t.Fatalf("code=%d loads=%d", code, p.mocks["A"].Counts().LoadStarts)
	}
}

func TestUnloadTimeoutThenLaterUnloadedLoadsSameRequest(t *testing.T) {
	gate := testkit.NewBarrier()
	p := newPair(t, []string{"A", "B"}, 100, 80, map[string][]mock.Option{
		"B": {mock.WithUnloadGate(gate)},
	})
	p.cfg.Models["A"] = withPeak(p.cfg.Models["A"], 60, 5)
	p.cfg.Models["B"] = withPeak(p.cfg.Models["B"], 60, 5)
	m := p.cfg.Models["B"]
	m.UnloadTimeout = 200 * time.Millisecond
	p.cfg.Models["B"] = m
	p.mocks["B"].ForceReady()
	p.reconcile()
	code, errCode, _ := serve(p.rt, "A")
	if code != http.StatusBadGateway || errCode != "UNLOAD_FAILED" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	gate.Release()
	waitUntil(t, 2*time.Second, func() bool {
		st := p.mocks["B"].Snapshot()
		return st.State == mock.StateUnloaded && st.Residency == mock.ResidencyNotResident
	})
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 80, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	base := p.mocks["B"].Counts().StatusGets
	done := make(chan struct{})
	go func() {
		code, errCode, _ := serve(p.rt, "A")
		if code != 200 {
			t.Errorf("code=%d err=%s", code, errCode)
		}
		close(done)
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.marked("B") })
	if p.mocks["B"].Counts().StatusGets <= base {
		t.Fatal("unloadFail was not probed before admission")
	}
	if p.reserved("B") != p.cfg.Models["B"].LoadBound() {
		t.Fatalf("reserved=%d", p.reserved("B"))
	}
	select {
	case <-done:
		t.Fatal("loaded before the post-status sample")
	default:
	}
	p.releaseSamples()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("same request did not load after the sample")
	}
	if p.mocks["A"].Counts().LoadStarts != 1 {
		t.Fatalf("loads=%d", p.mocks["A"].Counts().LoadStarts)
	}
}

func TestFailedNotResidentAllowsOtherLoadAfterSample(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	done := make(chan struct{})
	go func() {
		code, errCode, _ := serve(p.rt, "B")
		if code != 200 {
			t.Errorf("code=%d err=%s", code, errCode)
		}
		close(done)
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.marked("A") })
	diag := p.rt.Diagnostics()
	item := diag.Models["A"]
	if p.client["A"].State() != runtime.StateStopped || item.RemoteState != runtime.WireFailed || item.Reason != "recovered_remote_failed" {
		t.Fatalf("state=%s remote=%s reason=%s", p.client["A"].State(), item.RemoteState, item.Reason)
	}
	p.releaseSamples()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("other load did not proceed")
	}
	if p.mocks["B"].Counts().LoadStarts != 1 {
		t.Fatalf("loads=%d", p.mocks["B"].Counts().LoadStarts)
	}
}

func TestOccupiedAndBadStatusBlockOtherLoads(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyResident)
	p.reconcile()
	code, errCode, _ := serve(p.rt, "B")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" || p.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatalf("resident code=%d err=%s loads=%d", code, errCode, p.mocks["B"].Counts().LoadStarts)
	}
	if p.client["A"].State() != runtime.StateFailed || p.reserved("A") != p.peak("A") {
		t.Fatalf("state=%s reserved=%d", p.client["A"].State(), p.reserved("A"))
	}
	p.mocks["A"].SetStatusMode(mock.StatusUnreliable)
	code, errCode, _ = serve(p.rt, "B")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" || p.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatalf("probe code=%d err=%s", code, errCode)
	}
	p.mocks["A"].SetStatusMode(mock.StatusReadyNotResident)
	code, errCode, _ = serve(p.rt, "B")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" || p.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatalf("contradiction code=%d err=%s", code, errCode)
	}
	if p.client["A"].State() != runtime.StateFailed {
		t.Fatalf("state=%s", p.client["A"].State())
	}
}

func TestStaleFreshSampleDoesNotAdmit(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	done := make(chan struct{})
	go func() {
		serve(p.rt, "B")
		close(done)
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.marked("A") })
	select {
	case <-done:
		t.Fatal("fresh sample older than status admitted the load")
	case <-time.After(250 * time.Millisecond):
	}
	if p.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatal("load started")
	}
	p.releaseSamples()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("later sample did not admit")
	}
}

func TestSampleFailureIsNotReadyAndQueueTimeoutIs504(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	go func() {
		waitUntil(t, 2*time.Second, func() bool { return p.marked("A") })
		p.gpu.Fail(errors.New("nvml"))
		select {
		case p.rt.observeCh <- resource.Snapshot{}:
		default:
		}
	}()
	code, errCode, _ := serve(p.rt, "B")
	if code != http.StatusServiceUnavailable || errCode != "NOT_READY" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if !p.marked("A") || p.reserved("A") != p.peak("A") || p.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatalf("marked=%v reserved=%d", p.marked("A"), p.reserved("A"))
	}

	p2 := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p2.cfg.QueueTimeout = 400 * time.Millisecond
	p2.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p2.reconcile()
	p2.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	code, errCode, _ = serve(p2.rt, "B")
	if code != http.StatusGatewayTimeout || errCode != "QUEUE_TIMEOUT" {
		t.Fatalf("timeout code=%d err=%s", code, errCode)
	}
	if p2.client["A"].State() != runtime.StateStopped || !p2.marked("A") || p2.reserved("A") != p2.peak("A") {
		t.Fatalf("state=%s marked=%v reserved=%d", p2.client["A"].State(), p2.marked("A"), p2.reserved("A"))
	}
	status := p2.mocks["A"].Counts().StatusGets
	p2.cfg.QueueTimeout = 3 * time.Second
	done := make(chan struct{})
	go func() {
		code, errCode, _ := serve(p2.rt, "B")
		if code != 200 {
			t.Errorf("next code=%d err=%s", code, errCode)
		}
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	if p2.mocks["A"].Counts().StatusGets != status || p2.mocks["B"].Counts().LoadStarts != 0 || p2.mocks["A"].Counts().UnloadStarts != 0 {
		t.Fatal("next load restatused or evicted before the sample")
	}
	p2.releaseSamples()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sample did not unblock the next load")
	}
}

func TestCancelDuringSampleKeepsMarkerForNextLoad(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.cfg.Models["A"] = withPeak(p.cfg.Models["A"], 80, 1)
	p.cfg.Models["B"] = withPeak(p.cfg.Models["B"], 30, 1)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		serveCtx(p.rt, "B", ctx)
		close(done)
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.marked("A") })
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not return")
	}
	if p.client["A"].State() != runtime.StateStopped || !p.marked("A") || p.reserved("A") != p.peak("A") {
		t.Fatalf("state=%s marked=%v reserved=%d", p.client["A"].State(), p.marked("A"), p.reserved("A"))
	}
	var fits, rest bool
	p.rt.onLoop(func() {
		fits = p.rt.fits("B")
		rest = p.rt.fitsIfRestResidual("B")
	})
	if fits || rest {
		t.Fatalf("marker budget fits=%v rest=%v", fits, rest)
	}
}

func TestMarkerAndOtherFailedStatusThenDecide(t *testing.T) {
	p := newPair(t, []string{"A", "B", "C"}, 100, 100, nil)
	p.mocks["B"].ForceFailed(mock.ResidencyResident)
	p.reconcile()
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	p.rt.onLoop(func() { p.rt.residualAfter["A"] = time.Now() })
	p.mocks["B"].ForceReady()
	baseA := p.mocks["A"].Counts().StatusGets
	baseB := p.mocks["B"].Counts().StatusGets
	done := make(chan struct{})
	go func() {
		code, errCode, _ := serve(p.rt, "C")
		if code != 200 {
			t.Errorf("code=%d err=%s", code, errCode)
		}
		close(done)
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["B"].Counts().StatusGets > baseB && p.marked("A") })
	if p.mocks["A"].Counts().StatusGets != baseA {
		t.Fatal("marker model was status-probed again")
	}
	select {
	case <-done:
		t.Fatal("decided before A's sample")
	default:
	}
	p.releaseSamples()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("decide did not run after the sample")
	}
	if p.mocks["C"].Counts().LoadStarts != 1 || p.mocks["A"].Counts().StatusGets != baseA {
		t.Fatalf("Cload=%d Astatus=%d base=%d", p.mocks["C"].Counts().LoadStarts, p.mocks["A"].Counts().StatusGets, baseA)
	}
}

func TestReadyRecoveryDropsOldMarker(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	old := time.Now().Add(-time.Second)
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: old.Add(-time.Second)}, false)
	p.mocks["A"].HoldStatus()
	p.mocks["A"].ForceReady()
	base := p.mocks["A"].Counts().StatusGets
	done := make(chan int, 1)
	go func() {
		code, _, _ := serve(p.rt, "A")
		done <- code
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["A"].Counts().StatusGets > base })
	p.rt.onLoop(func() {
		p.rt.residualAfter["A"] = old
		p.rt.reserved["A"] = p.peak("A")
	})
	p.mocks["A"].ReleaseStatus()
	var code int
	select {
	case code = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ready recovery timed out")
	}
	if code != 200 || p.marked("A") {
		t.Fatalf("code=%d marked=%v", code, p.marked("A"))
	}
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now()}, false)
	snap, _ := p.gpu.Observe()
	p.rt.onLoop(func() { p.rt.applyObservation(snap) })
	if p.reserved("A") != p.peak("A") {
		t.Fatalf("peak dropped to %d", p.reserved("A"))
	}
}

func TestNonTargetReadyKeepsEvictionOrder(t *testing.T) {
	p := newPair(t, []string{"A", "B", "C"}, 100, 100, nil)
	p.cfg.Models["A"] = withPeak(p.cfg.Models["A"], 40, 1)
	p.cfg.Models["B"] = withPeak(p.cfg.Models["B"], 40, 1)
	p.cfg.Models["C"] = withPeak(p.cfg.Models["C"], 40, 1)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.mocks["B"].ForceReady()
	p.reconcile()
	past := time.Now().Add(-time.Hour)
	p.rt.onLoop(func() {
		p.rt.loadedAt["B"] = past
		p.rt.lastUse["B"] = past
		p.rt.reserved["B"] = 40
	})
	p.mocks["A"].ForceReady()
	code, errCode, _ := serve(p.rt, "C")
	if code != 200 {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if p.mocks["B"].Counts().UnloadStarts != 1 || p.mocks["A"].Counts().UnloadStarts != 0 {
		t.Fatalf("unload A=%d B=%d", p.mocks["A"].Counts().UnloadStarts, p.mocks["B"].Counts().UnloadStarts)
	}
	var lastA, loadedA time.Time
	p.rt.onLoop(func() {
		lastA = p.rt.lastUse["A"]
		loadedA = p.rt.loadedAt["A"]
	})
	if loadedA.IsZero() || !lastA.IsZero() {
		t.Fatalf("loadedAt=%s lastUse=%s", loadedA, lastA)
	}
	if !p.rt.Diagnostics().Models["B"].RemoteKnown && p.client["B"].State() != runtime.StateStopped && p.client["B"].State() != runtime.StateReady {
		t.Fatalf("B=%s", p.client["B"].State())
	}
}

func TestRecoveredReadyDoesNotNeedFreshGPUButOtherLoadDoes(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.cfg.GPU.MaxObservationAge = time.Second
	p.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-5 * time.Second)}, false)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.mocks["A"].ForceReady()
	code, _, _ := serve(p.rt, "A")
	if code != 200 || p.mocks["A"].Counts().LoadStarts != 0 {
		t.Fatalf("own inference code=%d loads=%d", code, p.mocks["A"].Counts().LoadStarts)
	}

	p2 := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p2.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p2.reconcile()
	p2.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(-2 * time.Second)}, false)
	go func() {
		waitUntil(t, 2*time.Second, func() bool { return p2.marked("A") })
		p2.gpu.Set(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now().Add(time.Minute)}, false)
		snap, _ := p2.gpu.Observe()
		select {
		case p2.rt.observeCh <- snap:
		default:
		}
	}()
	code, errCode, _ := serve(p2.rt, "B")
	if code != http.StatusServiceUnavailable || errCode != "NOT_READY" || p2.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatalf("other load code=%d err=%s loads=%d", code, errCode, p2.mocks["B"].Counts().LoadStarts)
	}
}

func TestFailedBusyIsResourcesAndReadyBusyIsLifecycle(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.mocks["A"].SetWire(mock.StateFailed, mock.ResidencyNotResident, 2)
	code, errCode, _ := serve(p.rt, "B")
	if code != http.StatusServiceUnavailable || errCode != "RESOURCES" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if p.client["A"].State() != runtime.StateFailed || p.reserved("A") != p.peak("A") || p.mocks["B"].Counts().LoadStarts != 0 {
		t.Fatalf("state=%s reserved=%d", p.client["A"].State(), p.reserved("A"))
	}

	p2 := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p2.mocks["B"].ForceFailed(mock.ResidencyNotResident)
	p2.reconcile()
	p2.mocks["B"].SetWire(mock.StateReady, mock.ResidencyResident, 1)
	code, errCode, hdr := serve(p2.rt, "A")
	if code != http.StatusServiceUnavailable || errCode != "LIFECYCLE_PENDING" || hdr.Get("Retry-After") != "" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if p2.reserved("B") != p2.peak("B") || p2.client["B"].State() != runtime.StateFailed {
		t.Fatalf("state=%s reserved=%d", p2.client["B"].State(), p2.reserved("B"))
	}
}

func TestReadyRequestDoesNotOvertakeRecoverHead(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.mocks["B"].ForceReady()
	p.reconcile()
	base := p.mocks["A"].Counts().StatusGets
	p.mocks["A"].HoldStatus()
	p.mocks["A"].ForceReady()
	aDone := make(chan struct{})
	go func() {
		serve(p.rt, "A")
		close(aDone)
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["A"].Counts().StatusGets > base })
	bDone := make(chan int, 1)
	go func() {
		code, _, _ := serve(p.rt, "B")
		bDone <- code
	}()
	select {
	case code := <-bDone:
		t.Fatalf("ready request overtook recover, code=%d", code)
	case <-time.After(200 * time.Millisecond):
	}
	p.mocks["A"].ReleaseStatus()
	select {
	case <-aDone:
	case <-time.After(2 * time.Second):
		t.Fatal("recover head did not finish")
	}
	select {
	case code := <-bDone:
		if code != 200 {
			t.Fatalf("B code=%d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ready head stayed blocked after the recover head finished")
	}
}

func TestJoinInFlightProbe(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	base := p.mocks["A"].Counts().StatusGets
	p.mocks["A"].HoldStatus()
	p.mocks["A"].ForceReady()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { serveCtx(p.rt, "A", ctx) }()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["A"].Counts().StatusGets > base })
	cancel()
	time.Sleep(50 * time.Millisecond)
	done := make(chan int, 1)
	go func() {
		code, _, _ := serve(p.rt, "A")
		done <- code
	}()
	time.Sleep(50 * time.Millisecond)
	p.mocks["A"].ReleaseStatus()
	select {
	case code := <-done:
		if code != 200 {
			t.Fatalf("code=%d", code)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("joined request did not finish")
	}
	if p.mocks["A"].Counts().LoadStarts != 0 {
		t.Fatal("joined recovery loaded again")
	}
	if p.mocks["A"].Counts().StatusGets > base+1 {
		t.Fatalf("status gets=%d base=%d", p.mocks["A"].Counts().StatusGets, base)
	}
}

func TestBusyApplyWhileLoadGateDoesNotDeadlock(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	entered := make(chan struct{})
	release := make(chan struct{})
	p.client["A"].SetLoadGate(func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	base := p.mocks["A"].Counts().StatusGets
	p.mocks["A"].HoldStatus()
	bDone := make(chan struct {
		code int
		err  string
	}, 1)
	go func() {
		code, errCode, _ := serve(p.rt, "B")
		bDone <- struct {
			code int
			err  string
		}{code, errCode}
	}()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["A"].Counts().StatusGets > base })
	ensured := make(chan error, 1)
	go func() { ensured <- p.client["A"].EnsureReady(context.Background(), 2*time.Second) }()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["A"].Counts().StatusGets > base+1 })
	p.mocks["A"].ReleaseStatus()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ensure did not reach the load gate")
	}
	var code int
	var errCode string
	select {
	case got := <-bDone:
		code, errCode = got.code, got.err
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not finish while the load gate was held")
	}
	if code != http.StatusServiceUnavailable || errCode != "LIFECYCLE_PENDING" {
		t.Fatalf("code=%d err=%s", code, errCode)
	}
	if p.client["A"].State() != runtime.StateStarting {
		t.Fatalf("state=%s", p.client["A"].State())
	}
	close(release)
	if err := <-ensured; err != nil {
		t.Fatal(err)
	}
}

func TestBootStartingJoinsExistingLoad(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, map[string][]mock.Option{
		"A": {mock.WithInitialLoading()},
	})
	p.reconcile()
	if p.client["A"].State() != runtime.StateStarting || !p.client["A"].LoadInFlight() {
		t.Fatalf("state=%s inflight=%v", p.client["A"].State(), p.client["A"].LoadInFlight())
	}
	done := make(chan int, 1)
	go func() {
		code, _, _ := serve(p.rt, "A")
		done <- code
	}()
	time.Sleep(100 * time.Millisecond)
	loads := p.mocks["A"].Counts().LoadStarts
	p.mocks["A"].ForceReady()
	select {
	case code := <-done:
		if code != 200 || p.mocks["A"].Counts().LoadStarts != loads {
			t.Fatalf("code=%d loads=%d", code, p.mocks["A"].Counts().LoadStarts)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("boot waiter did not complete")
	}
}

func TestShutdownDuringApplyRollsBackAndReleases(t *testing.T) {
	p := newPair(t, []string{"A"}, 100, 100, nil)
	p.mocks["A"].ForceFailed(mock.ResidencyNotResident)
	p.reconcile()
	p.mocks["A"].HoldStatus()
	p.mocks["A"].ForceReady()
	go func() { serve(p.rt, "A") }()
	waitUntil(t, 2*time.Second, func() bool { return p.mocks["A"].Counts().StatusGets > 0 })
	p.client["A"].Shutdown()
	time.Sleep(20 * time.Millisecond)
	p.mocks["A"].ReleaseStatus()
	time.Sleep(80 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.rt.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if p.client["A"].State() == runtime.StateReady {
		t.Fatal("shutdown apply left the client ready")
	}
	if p.rt.reserved["A"] == p.peak("A") && p.client["A"].State() != runtime.StateReady {
		t.Fatalf("tentative peak survived shutdown, reserved=%d state=%s", p.rt.reserved["A"], p.client["A"].State())
	}
}

func TestWaitApplyAckConsumesAckBeforeShutdown(t *testing.T) {
	for i := 0; i < 50; i++ {
		ack := make(chan runtime.ApplyCode, 1)
		shut := make(chan chan struct{}, 1)
		ack <- runtime.ApplyApplied
		done := make(chan struct{})
		shut <- done
		got, finished := waitApplyAck(ack, shut)
		if got != runtime.ApplyApplied || finished != done {
			t.Fatalf("i=%d ack=%v doneNil=%v", i, got, finished == nil)
		}
	}
}

func TestMarkerBudgetUsesPeak(t *testing.T) {
	p := newPair(t, []string{"A", "B"}, 100, 100, nil)
	p.cfg.Models["A"] = withPeak(p.cfg.Models["A"], 80, 1)
	p.cfg.Models["B"] = withPeak(p.cfg.Models["B"], 30, 1)
	p.rt.onLoop(func() {
		p.rt.residualAfter["A"] = time.Now()
		p.rt.reserved["A"] = 80
		p.rt.snap = resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now(), OK: true}
		p.rt.bootGPU = true
		if p.rt.fits("B") {
			t.Error("fits treated a marked model as residual")
		}
		if p.rt.fitsIfRestResidual("B") {
			t.Error("fitsIfRestResidual lowered a marked model")
		}
	})
}

func withPeak(m config.Model, peak, residual int64) config.Model {
	m.ResourceProfile = prof(peak, residual)
	return m
}
