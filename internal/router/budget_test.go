package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/resource"
	"github.com/kilingki/InferSwap/internal/runtime"
	"github.com/kilingki/InferSwap/internal/runtime/mock"
)

func TestTightBudgetEvictsIdle(t *testing.T) {
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
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	ma := cfg.Models["A"]
	ma.BaseURL = a.URL()
	ma.ResourceProfile = config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 80, InferencePeakBytes: 80, MaxConcurrency: 1}
	mb := cfg.Models["B"]
	mb.BaseURL = b.URL()
	mb.ResourceProfile = config.ResourceProfile{ProfileID: "b", LoadPeakBytes: 80, InferencePeakBytes: 80, MaxConcurrency: 1}
	cfg.Models["A"] = ma
	cfg.Models["B"] = mb
	obs := resource.Fake{Snap: resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, Used: 0, OK: true}}
	runtimes := map[string]runtime.Runtime{
		"A": runtime.NewClient(context.Background(), ma, cfg, runtime.ClientOptions{}),
		"B": runtime.NewClient(context.Background(), mb, cfg, runtime.ClientOptions{}),
	}
	rt := New(cfg, runtimes, nil, obs)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
		t.Fatal("A should be unloaded so B can fit")
	}
	if runtimes["B"].State() != runtime.StateReady {
		t.Fatalf("B state=%s", runtimes["B"].State())
	}
}

func TestObservationFailureBlocksLoad(t *testing.T) {
	a, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	m := cfg.Models["A"]
	m.BaseURL = a.URL()
	m.ResourceProfile = config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1}
	cfg.Models["A"] = m
	delete(cfg.Models, "B")
	obs := resource.Fake{Err: errString("nvml down")}
	c := runtime.NewClient(context.Background(), m, cfg, runtime.ClientOptions{})
	rt := New(cfg, map[string]runtime.Runtime{"A": c}, nil, obs)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
	}()
	if err := rt.Preload(context.Background(), []string{"A"}); err == nil {
		t.Fatal("load without a gpu snapshot")
	}
	if a.Counts().LoadStarts != 0 {
		t.Fatal("postLoad ran without admission")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestObservationFailureClearsFreshSample(t *testing.T) {
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	m := cfg.Models["A"]
	m.ResourceProfile = config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1}
	cfg.Models["A"] = m
	rt := New(cfg, map[string]runtime.Runtime{"A": &stubRT{state: runtime.StateStopped}}, nil, resource.Generous("fake"))
	defer shutdownRouter(t, rt)
	var fresh, stale bool
	var admit error
	rt.onLoop(func() {
		rt.applyObservation(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now(), OK: true})
		fresh = rt.gpuFresh()
		rt.applyObservation(resource.Snapshot{})
		stale = rt.gpuFresh()
		admit = rt.grantLoad("A")
	})
	if !fresh {
		t.Fatal("expected a fresh sample")
	}
	if stale {
		t.Fatal("failed observation left the previous sample admissible")
	}
	if admit == nil {
		t.Fatal("admitted a load after observation failure")
	}
}

func TestDuplicateFreeIsNotAdmittedTwice(t *testing.T) {
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	for _, id := range []string{"A", "B"} {
		m := cfg.Models[id]
		m.ResourceProfile = config.ResourceProfile{ProfileID: id, LoadPeakBytes: 80, InferencePeakBytes: 80, MaxConcurrency: 1}
		cfg.Models[id] = m
	}
	rt := New(cfg, map[string]runtime.Runtime{
		"A": &stubRT{state: runtime.StateStopped},
		"B": &stubRT{state: runtime.StateStopped},
	}, nil, resource.Generous("fake"))
	defer shutdownRouter(t, rt)
	var first, second error
	rt.onLoop(func() {
		rt.applyObservation(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: time.Now(), OK: true})
		first = rt.grantLoad("A")
		second = rt.grantLoad("B")
	})
	if first != nil {
		t.Fatal(first)
	}
	if second == nil {
		t.Fatal("second load admitted against the same free sample")
	}
}

func TestOldSampleDoesNotDropToResidual(t *testing.T) {
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	m := cfg.Models["A"]
	m.ResourceProfile = config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 80, InferencePeakBytes: 80, UnloadedResidualBytes: 5, MaxConcurrency: 1}
	cfg.Models["A"] = m
	rt := New(cfg, map[string]runtime.Runtime{"A": &stubRT{state: runtime.StateReady}}, nil, resource.Generous("fake"))
	defer shutdownRouter(t, rt)
	unloadAt := time.Now()
	var before, after int64
	rt.onLoop(func() {
		rt.reserved["A"] = 80
		rt.residualAfter["A"] = unloadAt
		rt.applyObservation(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: unloadAt.Add(-time.Second), OK: true})
		before = rt.reserved["A"]
		rt.applyObservation(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, ObservedAt: unloadAt.Add(time.Second), OK: true})
		after = rt.reserved["A"]
	})
	if before != 80 {
		t.Fatalf("old sample changed reservation to %d", before)
	}
	if after != 5 {
		t.Fatalf("residual=%d", after)
	}
}

func TestStartingIsNotEvicted(t *testing.T) {
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	for _, id := range []string{"A", "B"} {
		m := cfg.Models[id]
		m.ResourceProfile = config.ResourceProfile{ProfileID: id, LoadPeakBytes: 80, InferencePeakBytes: 80, MaxConcurrency: 1}
		cfg.Models[id] = m
	}
	a := &stubRT{state: runtime.StateStarting}
	b := &stubRT{state: runtime.StateStopped}
	rt := New(cfg, map[string]runtime.Runtime{"A": a, "B": b}, nil, resource.Fake{Snap: resource.Snapshot{DeviceID: "fake", Total: 100, Free: 90, OK: true}})
	defer shutdownRouter(t, rt)
	if err := rt.Preload(context.Background(), []string{"B"}); err == nil {
		t.Fatal("loaded B by evicting a starting model")
	}
	if a.stops != 0 {
		t.Fatalf("starting model unloaded, stops=%d", a.stops)
	}
}

func TestContinuousEviction(t *testing.T) {
	mk := func(name string) (*mock.Server, config.Model) {
		t.Helper()
		s, err := mock.New(name)
		if err != nil {
			t.Fatal(err)
		}
		m := config.Model{ID: name, BaseURL: s.URL(), ConcurrencyLimit: 1, HealthCheckTimeout: time.Second, UnloadTimeout: time.Second}
		return s, m
	}
	as, ma := mk("A")
	defer as.Close()
	bs, mb := mk("B")
	defer bs.Close()
	cs, mc := mk("C")
	defer cs.Close()
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	ma.ResourceProfile = config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 40, InferencePeakBytes: 40, MaxConcurrency: 1}
	mb.ResourceProfile = config.ResourceProfile{ProfileID: "b", LoadPeakBytes: 40, InferencePeakBytes: 40, MaxConcurrency: 1}
	mc.ResourceProfile = config.ResourceProfile{ProfileID: "c", LoadPeakBytes: 80, InferencePeakBytes: 80, MaxConcurrency: 1}
	cfg.Models["A"] = ma
	cfg.Models["B"] = mb
	cfg.Models["C"] = mc
	runtimes := map[string]runtime.Runtime{
		"A": runtime.NewClient(context.Background(), ma, cfg, runtime.ClientOptions{}),
		"B": runtime.NewClient(context.Background(), mb, cfg, runtime.ClientOptions{}),
		"C": runtime.NewClient(context.Background(), mc, cfg, runtime.ClientOptions{}),
	}
	obs := resource.Fake{Snap: resource.Snapshot{DeviceID: "fake", Total: 100, Free: 100, OK: true}}
	rt := New(cfg, runtimes, nil, obs)
	defer shutdownRouter(t, rt)
	if err := rt.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rt.Preload(context.Background(), []string{"A", "B"}); err != nil {
		t.Fatal(err)
	}
	if runtimes["A"].State() != runtime.StateReady || runtimes["B"].State() != runtime.StateReady {
		t.Fatalf("A=%s B=%s", runtimes["A"].State(), runtimes["B"].State())
	}
	if err := rt.Preload(context.Background(), []string{"C"}); err != nil {
		t.Fatal(err)
	}
	if runtimes["A"].State() == runtime.StateReady || runtimes["B"].State() == runtime.StateReady {
		t.Fatalf("both should be evicted A=%s B=%s", runtimes["A"].State(), runtimes["B"].State())
	}
	if runtimes["C"].State() != runtime.StateReady {
		t.Fatalf("C=%s", runtimes["C"].State())
	}
}

func TestLowFreeStillEvictsWhenTotalCanHoldTarget(t *testing.T) {
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	for _, id := range []string{"A", "B"} {
		m := cfg.Models[id]
		peak := int64(80)
		if id == "B" {
			peak = 40
		}
		m.ResourceProfile = config.ResourceProfile{ProfileID: id, LoadPeakBytes: peak, InferencePeakBytes: peak, MaxConcurrency: 1}
		cfg.Models[id] = m
	}
	rt := New(cfg, map[string]runtime.Runtime{
		"A": &stubRT{state: runtime.StateReady},
		"B": &stubRT{state: runtime.StateStopped},
	}, nil, resource.Generous("fake"))
	defer shutdownRouter(t, rt)
	var evict []string
	var err error
	rt.onLoop(func() {
		rt.applyObservation(resource.Snapshot{DeviceID: "fake", Total: 100, Free: 20, Used: 80, ObservedAt: time.Now(), OK: true})
		evict, err = rt.Decide("B", []string{"A"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(evict) != 1 || evict[0] != "A" {
		t.Fatalf("evict=%v", evict)
	}
}

func TestDrainTimeoutDoesNotUnload(t *testing.T) {
	cfg := testRouterCfg()
	cfg.GPU.Device = "fake"
	cfg.GPU.MaxObservationAge = time.Minute
	cfg.DrainTimeout = 20 * time.Millisecond
	for _, id := range []string{"A", "B"} {
		m := cfg.Models[id]
		m.ResourceProfile = config.ResourceProfile{ProfileID: id, LoadPeakBytes: 80, InferencePeakBytes: 80, MaxConcurrency: 1}
		cfg.Models[id] = m
	}
	a := &stubRT{state: runtime.StateReady, status: runtime.Status{ActiveRequests: 1, State: runtime.WireReady}}
	b := &stubRT{state: runtime.StateStopped}
	rt := New(cfg, map[string]runtime.Runtime{"A": a, "B": b}, nil, resource.Fake{Snap: resource.Snapshot{DeviceID: "fake", Total: 100, Free: 90, OK: true}})
	defer shutdownRouter(t, rt)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	rt.ServeModel("B", w, req)
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
	if a.stops != 0 {
		t.Fatalf("unloaded despite drain timeout, stops=%d", a.stops)
	}
}

func shutdownRouter(t *testing.T, rt *Router) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = rt.Shutdown(ctx)
}
