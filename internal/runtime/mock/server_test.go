package mock

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/testkit"
)

func TestIndependentInstances(t *testing.T) {
	a, err := New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := New("B")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	loadOK(t, a.URL())
	stA := getStatus(t, a.URL())
	stB := getStatus(t, b.URL())
	if stA["state"] != "ready" || stA["residency"] != "resident" {
		t.Fatalf("A status=%v", stA)
	}
	if stB["state"] != "unloaded" || stB["residency"] != "not_resident" {
		t.Fatalf("B should stay unloaded, got %v", stB)
	}
	if a.Counts().LoadStarts == 0 || b.Counts().LoadStarts != 0 {
		t.Fatalf("counts leaked: A=%+v B=%+v", a.Counts(), b.Counts())
	}
}

func TestUnloadedStatusContract(t *testing.T) {
	s, err := New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	st := getStatus(t, s.URL())
	if st["state"] != "unloaded" || st["residency"] != "not_resident" || st["active_requests"] != float64(0) || st["last_error"] != nil {
		t.Fatalf("status=%v", st)
	}
	for _, gone := range []string{"protocol_version", "model_state", "inference_ready", "resident", "resource", "environment_ready"} {
		if _, ok := st[gone]; ok {
			t.Fatalf("unexpected field %s", gone)
		}
	}
	resp, err := http.Get(s.URL() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("runtime /health=%d", resp.StatusCode)
	}
}

func TestLoadThenUnloadContract(t *testing.T) {
	s, err := New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	loadOK(t, s.URL())
	st := getStatus(t, s.URL())
	if st["state"] != "ready" || st["residency"] != "resident" || st["active_requests"] != float64(0) {
		t.Fatalf("after load: %+v", st)
	}
	inferResp, err := http.Post(s.URL()+"/v1/chat/completions", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	inferResp.Body.Close()
	if inferResp.StatusCode != http.StatusOK {
		t.Fatalf("infer=%d", inferResp.StatusCode)
	}

	unloadOK(t, s.URL())
	st = getStatus(t, s.URL())
	if st["state"] != "unloaded" || st["residency"] != "not_resident" {
		t.Fatalf("after unload: %+v", st)
	}
	if st["active_requests"] != float64(0) {
		t.Fatalf("active_requests=%v", st["active_requests"])
	}
}

func TestBusyUnloadConflict(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := New("A", WithInferGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loadOK(t, s.URL())

	errCh := make(chan error, 1)
	go func() {
		_, err := http.Post(s.URL()+"/v1/chat/completions", "application/json", bytes.NewBufferString(`{}`))
		errCh <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	if got := s.Snapshot(); got.ActiveRequests != 1 {
		t.Fatalf("active=%v", got.ActiveRequests)
	}

	resp, err := http.Post(s.URL()+"/control/unload", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	var ce struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ce); err != nil {
		t.Fatal(err)
	}
	if ce.Error.Code != "BUSY" || ce.Error.Message == "" {
		t.Fatalf("error=%+v", ce)
	}

	gate.Release()
	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("inference did not finish")
	}
}

func TestInferenceGateHoldsActive(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := New("A", WithInferGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loadOK(t, s.URL())

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.Post(s.URL()+"/v1/completions", "application/json", bytes.NewBufferString(`{}`))
		if err != nil {
			t.Error(err)
			return
		}
		resp.Body.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if snap.ActiveRequests != 1 {
		t.Fatalf("active during gate=%v", snap.ActiveRequests)
	}
	if s.Counts().InferEnds != 0 {
		t.Fatal("infer ended before release")
	}
	gate.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	snap = s.Snapshot()
	if snap.ActiveRequests != 0 {
		t.Fatalf("active after=%v", snap.ActiveRequests)
	}
}

func TestDisconnectKeepsBackendActive(t *testing.T) {
	infer := testkit.NewBarrier()
	backend := testkit.NewBarrier()
	s, err := New("A", WithInferGate(infer), WithBackendGate(backend), WithPersistInference())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loadOK(t, s.URL())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.URL()+"/v1/chat/completions", bytes.NewBufferString(`{}`))
		_, err := http.DefaultClient.Do(req)
		done <- err
	}()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := infer.WaitStarted(waitCtx); err != nil {
		t.Fatal(err)
	}
	cancel()
	infer.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after cancel")
	}
	snap := s.Snapshot()
	if snap.ActiveRequests != 1 {
		t.Fatalf("active after handler return=%v", snap.ActiveRequests)
	}
	backend.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snap = s.Snapshot()
		if snap.ActiveRequests == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("active never dropped: %v", s.Snapshot().ActiveRequests)
}

func TestPrepareOpensDownEndpoint(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := New("A", WithEndpointDown(), WithPrepareGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = http.Get(s.URL() + "/control/status")
	if err == nil {
		t.Fatal("expected down endpoint to fail")
	}

	opened := make(chan error, 1)
	go func() {
		opened <- s.Prepare(context.Background())
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	if s.EndpointAlive() {
		t.Fatal("endpoint opened before prepare release")
	}
	gate.Release()
	select {
	case err := <-opened:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("prepare hung")
	}
	st := getStatus(t, s.URL())
	if st["state"] != "unloaded" || st["residency"] != "not_resident" || st["active_requests"] != float64(0) {
		t.Fatalf("state=%v", st)
	}
}

func TestLoadFailureAndLateCompleteAfterCancel(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := New("A", WithLoadGate(gate), WithLoadFailure())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.URL()+"/control/load", bytes.NewBufferString(`{}`))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- -1
			return
		}
		done <- resp.StatusCode
		resp.Body.Close()
	}()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := gate.WaitStarted(waitCtx); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("load handler did not return")
	}
	if s.Snapshot().State != StateLoading {
		t.Fatalf("state after cancel=%s", s.Snapshot().State)
	}
	gate.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.Snapshot().State == StateFailed {
			if s.Counts().LoadEnds != 1 {
				t.Fatalf("load ends=%d", s.Counts().LoadEnds)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("late failure not published: %s", s.Snapshot().State)
}

func TestStatusMalformedAndUnreliable(t *testing.T) {
	s, err := New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.SetStatusMode(StatusMalformed)
	raw := getRaw(t, s.URL()+"/control/status")
	if json.Valid(raw) {
		t.Fatalf("expected malformed json, got %s", raw)
	}

	s.SetStatusMode(StatusUnreliable)
	resp, err := http.Get(s.URL() + "/control/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestLifecycleConflictsAndIdempotent(t *testing.T) {
	loadGate := testkit.NewBarrier()
	s, err := New("A", WithLoadGate(loadGate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	go func() {
		_, _ = http.Post(s.URL()+"/control/load", "application/json", bytes.NewBufferString(`{}`))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := loadGate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(s.URL()+"/control/unload", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict || !bytes.Contains(body, []byte("LIFECYCLE_CONFLICT")) {
		t.Fatalf("unload during load: %d %s", resp.StatusCode, body)
	}
	st := getStatus(t, s.URL())
	if st["state"] != "loading" {
		t.Fatalf("status during load=%v", st)
	}
	loadGate.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && s.Snapshot().State != StateReady {
		time.Sleep(5 * time.Millisecond)
	}
	if s.Snapshot().State != StateReady {
		t.Fatalf("state=%s", s.Snapshot().State)
	}
	before := s.Counts().LoadStarts
	loadOK(t, s.URL())
	if s.Counts().LoadStarts != before {
		t.Fatal("ready load must be a no-op")
	}
	unloadOK(t, s.URL())
	before = s.Counts().UnloadStarts
	unloadOK(t, s.URL())
	if s.Counts().UnloadStarts != before {
		t.Fatal("unloaded unload must be a no-op")
	}
}

func TestUnloadBeforeInferenceRejects(t *testing.T) {
	gate := testkit.NewBarrier()
	s, err := New("A", WithUnloadGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loadOK(t, s.URL())
	go func() {
		_, _ = http.Post(s.URL()+"/control/unload", "application/json", bytes.NewBufferString(`{}`))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(s.URL()+"/v1/chat/completions", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("infer during unload=%d", resp.StatusCode)
	}
	if s.Snapshot().ActiveRequests != 0 {
		t.Fatalf("active=%d", s.Snapshot().ActiveRequests)
	}
	gate.Release()
}

func TestPreparePreservesReady(t *testing.T) {
	s, err := New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loadOK(t, s.URL())
	if err := s.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := s.Snapshot()
	if st.State != StateReady || st.Residency != ResidencyResident {
		t.Fatalf("prepare reset ready model: %+v", st)
	}
}

func loadOK(t *testing.T, base string) {
	t.Helper()
	resp, err := http.Post(base+"/control/load", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("load status=%d body=%s", resp.StatusCode, body)
	}
}

func unloadOK(t *testing.T, base string) {
	t.Helper()
	resp, err := http.Post(base+"/control/unload", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unload status=%d body=%s", resp.StatusCode, body)
	}
}

func getStatus(t *testing.T, base string) map[string]any {
	t.Helper()
	raw := getRaw(t, base+"/control/status")
	var st map[string]any
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

func getRaw(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
