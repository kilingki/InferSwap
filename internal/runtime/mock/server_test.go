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
	if stA["model_state"] != "ready" {
		t.Fatalf("A state=%v", stA["model_state"])
	}
	if stB["model_state"] != "unloaded" {
		t.Fatalf("B should stay unloaded, got %v", stB["model_state"])
	}
	if a.Counts().LoadStarts == 0 || b.Counts().LoadStarts != 0 {
		t.Fatalf("counts leaked: A=%+v B=%+v", a.Counts(), b.Counts())
	}
}

func TestUnloadedStatusNullsPreserved(t *testing.T) {
	s, err := New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	raw := getRaw(t, s.URL()+"/control/status")
	var st map[string]any
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if st["protocol_version"] != "v0" {
		t.Fatalf("protocol_version=%v", st["protocol_version"])
	}
	if st["environment_ready"] != true {
		t.Fatalf("environment_ready=%v", st["environment_ready"])
	}
	if st["model_state"] != "unloaded" {
		t.Fatalf("model_state=%v", st["model_state"])
	}
	if st["inference_ready"] != false {
		t.Fatal("inference_ready")
	}
	if st["resident"] != false {
		t.Fatalf("resident=%v", st["resident"])
	}
	if st["active_requests"] != float64(0) {
		t.Fatalf("active_requests=%v", st["active_requests"])
	}
	if st["last_error"] != nil {
		t.Fatalf("last_error=%v", st["last_error"])
	}
	res := st["resource"].(map[string]any)
	for _, k := range []string{"device_id", "observed_bytes", "unloaded_residual_bytes", "budget"} {
		if res[k] != nil {
			t.Fatalf("resource.%s should be null, got %v", k, res[k])
		}
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
	if st["inference_ready"] != true || st["resident"] != true || st["model_state"] != "ready" {
		t.Fatalf("after load: %+v", st)
	}
	resp, err := http.Get(s.URL() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health=%d", resp.StatusCode)
	}

	unloadOK(t, s.URL())
	st = getStatus(t, s.URL())
	if st["inference_ready"] != false || st["resident"] != false || st["model_state"] != "unloaded" {
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
	if got := s.Snapshot(); got.ActiveRequests == nil || *got.ActiveRequests != 1 {
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
	var ce ControlError
	if err := json.NewDecoder(resp.Body).Decode(&ce); err != nil {
		t.Fatal(err)
	}
	if ce.Code != "BUSY" || ce.Src != "runtime" {
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
	if snap.ActiveRequests == nil || *snap.ActiveRequests != 1 {
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
	if snap.ActiveRequests == nil || *snap.ActiveRequests != 0 {
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
	if snap.ActiveRequests == nil || *snap.ActiveRequests != 1 {
		t.Fatalf("active after handler return=%v", snap.ActiveRequests)
	}
	backend.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snap = s.Snapshot()
		if snap.ActiveRequests != nil && *snap.ActiveRequests == 0 {
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
	if st["model_state"] != "unloaded" {
		t.Fatalf("state=%v", st["model_state"])
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
	if s.Snapshot().ModelState != StateLoading {
		t.Fatalf("state after cancel=%s", s.Snapshot().ModelState)
	}
	gate.Release()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.Snapshot().ModelState == StateFailed {
			if s.Counts().LoadEnds != 1 {
				t.Fatalf("load ends=%d", s.Counts().LoadEnds)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("late failure not published: %s", s.Snapshot().ModelState)
}

func TestStatusUnknownAndMalformed(t *testing.T) {
	s, err := New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	s.SetStatusMode(StatusUnknown)
	raw := getRaw(t, s.URL()+"/control/status")
	var st map[string]any
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if st["model_state"] != "unknown" {
		t.Fatalf("state=%v", st["model_state"])
	}
	if st["resident"] != nil || st["active_requests"] != nil {
		t.Fatalf("unknown should use nulls: %+v", st)
	}

	s.SetStatusMode(StatusMalformed)
	raw = getRaw(t, s.URL()+"/control/status")
	if json.Valid(raw) {
		t.Fatalf("expected malformed json, got %s", raw)
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
