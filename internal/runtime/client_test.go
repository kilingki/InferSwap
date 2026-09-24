package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/runtime/mock"
	"github.com/kilingki/InferSwap/internal/testkit"
)

func testGlobals() *config.Config {
	return &config.Config{StatusTimeout: 2 * time.Second}
}

func testModel(base string) config.Model {
	return config.Model{
		ID:                 "a",
		BaseURL:            base,
		ConcurrencyLimit:   1,
		PrepareTimeout:     2 * time.Second,
		HealthCheckTimeout: 3 * time.Second,
		UnloadTimeout:      2 * time.Second,
	}
}

type recCommander struct {
	mu   sync.Mutex
	runs int
	cwd  string
	env  []string
	fn   func(ctx context.Context, argv []string, cwd string) error
}

func (r *recCommander) Run(ctx context.Context, argv []string, cwd string) error {
	r.mu.Lock()
	r.runs++
	r.cwd = cwd
	r.env = append([]string{}, os.Environ()...)
	fn := r.fn
	r.mu.Unlock()
	if fn != nil {
		return fn(ctx, argv, cwd)
	}
	return nil
}

func (r *recCommander) Runs() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runs
}

func newClient(t *testing.T, s *mock.Server, model config.Model, opts ClientOptions) *Client {
	t.Helper()
	c := NewClient(context.Background(), model, testGlobals(), opts)
	t.Cleanup(c.Shutdown)
	return c
}

func TestEnsureReadyFromStatus(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.EnsureReady(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateReady {
		t.Fatalf("state=%s", c.State())
	}
	st := s.Snapshot()
	if st.State != mock.StateReady || st.Residency != mock.ResidencyResident {
		t.Fatalf("remote=%+v", st)
	}
	resp, err := http.Get(s.URL() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("readiness must not depend on runtime /health")
	}
}

func TestConcurrentEnsureReadyJoinsOneLoad(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := mock.New("A", mock.WithLoadGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.EnsureReady(context.Background(), 0)
		}(i)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Counts().LoadStarts != 1 {
		t.Fatalf("load starts=%d", s.Counts().LoadStarts)
	}
	gate.Release()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("EnsureReady %d: %v", i, err)
		}
	}
}

func TestStopRespectsUnloadTimeout(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := mock.New("A", mock.WithUnloadGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.EnsureReady(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- c.Stop(context.Background(), 50*time.Millisecond)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected unload timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not bound unload timeout")
	}
	gate.Release()
}

func TestEnsureReadyAfterStoppingLoads(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := mock.New("A", mock.WithUnloadGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.EnsureReady(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- c.Stop(context.Background(), 2*time.Second) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}

	readyDone := make(chan error, 1)
	go func() { readyDone <- c.EnsureReady(context.Background(), 0) }()
	time.Sleep(20 * time.Millisecond)
	if s.Counts().LoadStarts != 1 {
		t.Fatalf("load during stop=%d", s.Counts().LoadStarts)
	}
	gate.Release()
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if err := <-readyDone; err != nil {
		t.Fatal(err)
	}
	if s.Counts().LoadStarts != 2 {
		t.Fatalf("expected load after stop, starts=%d", s.Counts().LoadStarts)
	}
	if c.State() != StateReady {
		t.Fatalf("state=%s", c.State())
	}
}

func TestLoadingDeadlineDoesNotMarkReady(t *testing.T) {
	s, err := mock.New("A", mock.WithInitialLoading())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	clock := testkit.NewFakeClock(time.Time{})
	c := newClient(t, s, testModel(s.URL()), ClientOptions{Clock: clock})

	done := make(chan error, 1)
	go func() { done <- c.EnsureReady(context.Background(), 0) }()
	for i := 0; i < 4; i++ {
		time.Sleep(20 * time.Millisecond)
		clock.Advance(time.Second)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ready deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("EnsureReady hung")
	}
	if c.State() == StateReady {
		t.Fatal("must not mark ready after status deadline")
	}
	if s.Counts().LoadStarts != 0 {
		t.Fatal("joining an in-progress load must not start another")
	}
}

func TestReadyReverseProxy(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.EnsureReady(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	rr := httptest.NewRecorder()
	c.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.Bytes())
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["id"] != "mock" {
		t.Fatalf("body=%v", body)
	}
}

func TestUnknownDoesNotAssumeReadyOrStopped(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetStatusMode(mock.StatusReadyNotResident)
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	err = c.EnsureReady(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for unknown remote state")
	}
	if c.State() == StateReady || c.State() == StateStopped {
		t.Fatalf("state=%s", c.State())
	}
	if s.Counts().LoadStarts != 0 {
		t.Fatal("blind load from unknown")
	}
}

func TestFailedNotResidentAllowsReload(t *testing.T) {
	s, err := mock.New("A", mock.WithLoadFailure())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.EnsureReady(context.Background(), 0); err == nil {
		t.Fatal("expected load failure")
	}
	if s.Counts().LoadStarts != 1 {
		t.Fatalf("starts=%d", s.Counts().LoadStarts)
	}
	s.SetLoadFailure(false)
	if err := c.EnsureReady(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if s.Counts().LoadStarts != 2 {
		t.Fatalf("recovery load starts=%d", s.Counts().LoadStarts)
	}
	if c.State() != StateReady {
		t.Fatalf("state=%s", c.State())
	}
}

func TestFailedUnknownBlocksLoadAllowsRecoveryUnload(t *testing.T) {
	s, err := mock.New("A", mock.WithLoadFailureUnknown())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.EnsureReady(context.Background(), 0); err == nil {
		t.Fatal("expected load failure")
	}
	if err := c.EnsureReady(context.Background(), 0); err == nil {
		t.Fatal("failed+unknown must not load")
	}
	if s.Counts().LoadStarts != 1 {
		t.Fatalf("load starts=%d", s.Counts().LoadStarts)
	}
	if err := c.Stop(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	st := s.Snapshot()
	if st.State != mock.StateUnloaded || st.Residency != mock.ResidencyNotResident || st.LastError != nil {
		t.Fatalf("recovery=%+v", st)
	}
}

func TestPrepareSkippedWhenEndpointAlive(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec := &recCommander{fn: func(context.Context, []string, string) error {
		t.Error("prepare should not run")
		return nil
	}}
	model := testModel(s.URL())
	model.Prepare = &config.Prepare{Argv: []string{"/abs/prepare"}}
	c := newClient(t, s, model, ClientOptions{Commander: rec})
	if err := c.EnsureReady(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if rec.Runs() != 0 {
		t.Fatalf("prepare runs=%d", rec.Runs())
	}
}

func TestPrepareOnceWhenEndpointDown(t *testing.T) {
	s, err := mock.New("A", mock.WithEndpointDown())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec := &recCommander{fn: func(ctx context.Context, argv []string, cwd string) error {
		return s.Prepare(ctx)
	}}
	model := testModel(s.URL())
	model.Prepare = &config.Prepare{Argv: []string{"/abs/prepare"}}
	c := newClient(t, s, model, ClientOptions{Commander: rec})

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.EnsureReady(context.Background(), 0)
		}()
	}
	wg.Wait()
	if rec.Runs() != 1 {
		t.Fatalf("prepare runs=%d", rec.Runs())
	}
	if s.Counts().LoadStarts != 1 {
		t.Fatalf("load starts=%d", s.Counts().LoadStarts)
	}
}

func TestPrepareCwdAndEnv(t *testing.T) {
	s, err := mock.New("A", mock.WithEndpointDown())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	argv0 := filepath.Join(t.TempDir(), "bin", "prepare")
	if err := os.MkdirAll(filepath.Dir(argv0), 0o755); err != nil {
		t.Fatal(err)
	}
	rec := &recCommander{fn: func(ctx context.Context, argv []string, cwd string) error {
		return s.Prepare(ctx)
	}}
	model := testModel(s.URL())
	model.Prepare = &config.Prepare{Argv: []string{argv0}}
	c := newClient(t, s, model, ClientOptions{Commander: rec})
	if err := c.EnsureReady(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if rec.cwd != filepath.Dir(argv0) {
		t.Fatalf("cwd=%s want %s", rec.cwd, filepath.Dir(argv0))
	}
	found := false
	for _, e := range rec.env {
		if len(e) > 5 && e[:5] == "PATH=" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected process environment to be inherited")
	}
}

func TestPrepareUnavailable(t *testing.T) {
	s, err := mock.New("A", mock.WithEndpointDown())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	err = c.EnsureReady(context.Background(), 0)
	if err != ErrPrepareUnavailable {
		t.Fatalf("err=%v", err)
	}
}

func TestStatusRejectsNullAndContradictions(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	for _, mode := range []mock.StatusMode{mock.StatusNullActive, mock.StatusNegativeActive, mock.StatusMissingActive, mock.StatusReadyNotResident, mock.StatusMalformed} {
		s.SetStatusMode(mode)
		if _, err := c.Status(context.Background()); err == nil {
			t.Fatalf("mode %d accepted", mode)
		}
	}
	s.SetStatusMode(mock.StatusNormal)
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.State != WireUnloaded || st.Residency != ResidencyNotResident || st.ActiveRequests != 0 {
		t.Fatalf("status=%+v", st)
	}
}

func TestShutdownRejectsEnsureReady(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := NewClient(context.Background(), testModel(s.URL()), testGlobals(), ClientOptions{})
	c.Shutdown()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && c.State() != StateShutdown {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.EnsureReady(context.Background(), 0); err == nil {
		t.Fatal("expected shutdown error")
	}
}

func TestCancelDoesNotAbortStartedLoad(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := mock.New("A", mock.WithLoadGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.EnsureReady(ctx, 0) }()
	wait, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := gate.WaitStarted(wait); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled caller should error")
		}
	case <-time.After(time.Second):
		t.Fatal("caller did not return")
	}
	gate.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.Counts().LoadEnds == 1 && s.Snapshot().State == mock.StateReady {
			if c.State() != StateReady {
				t.Fatalf("operation should finish ready, state=%s", c.State())
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("started load did not finish: counts=%+v state=%s remote=%s", s.Counts(), c.State(), s.Snapshot().State)
}

func TestPrepareTimeoutAndNoBlindRerun(t *testing.T) {
	s, err := mock.New("A", mock.WithEndpointDown())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	clock := testkit.NewFakeClock(time.Time{})
	var started atomic.Bool
	rec := &recCommander{fn: func(ctx context.Context, argv []string, cwd string) error {
		started.Store(true)
		<-ctx.Done()
		return ctx.Err()
	}}
	model := testModel(s.URL())
	model.Prepare = &config.Prepare{Argv: []string{"/abs/prepare"}}
	model.PrepareTimeout = time.Second
	c := newClient(t, s, model, ClientOptions{Clock: clock, Commander: rec})
	done := make(chan error, 1)
	go func() { done <- c.EnsureReady(context.Background(), 0) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && !started.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	clock.Advance(time.Second)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected prepare timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("hung")
	}
	if rec.Runs() != 1 {
		t.Fatalf("runs=%d", rec.Runs())
	}
	if err := c.EnsureReady(context.Background(), 0); err == nil {
		t.Fatal("second prepare must wait for the first child")
	}
	if rec.Runs() != 1 {
		t.Fatalf("rerun=%d", rec.Runs())
	}
}
