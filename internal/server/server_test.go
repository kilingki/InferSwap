package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/router"
	"github.com/kilingki/InferSwap/internal/runtime"
	"github.com/kilingki/InferSwap/internal/runtime/mock"
	"github.com/kilingki/InferSwap/internal/testkit"
)

func newTestStack(t *testing.T, a, b *mock.Server) http.Handler {
	t.Helper()
	cfg := &config.Config{
		MaxQueueSize:       16,
		QueueTimeout:       time.Minute,
		StatusTimeout:      2 * time.Second,
		UnloadTimeout:      2 * time.Second,
		HealthCheckTimeout: 5 * time.Second,
		Models: map[string]config.Model{
			"model-a": {
				ID: "model-a", Name: "A", BaseURL: a.URL(), Aliases: []string{"alias-a"},
				ConcurrencyLimit: 1, HealthCheckTimeout: 5 * time.Second, UnloadTimeout: 2 * time.Second,
				InferencePath:    "/v1/audio/transcriptions",
				MaxBodyBytes:    32 << 20,
				ResourceProfile: config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
			"model-b": {
				ID: "model-b", Name: "B", BaseURL: b.URL(),
				ConcurrencyLimit: 1, HealthCheckTimeout: 5 * time.Second, UnloadTimeout: 2 * time.Second, Unlisted: true,
				InferencePath:    "/align",
				MaxBodyBytes:    32 << 20,
				ResourceProfile: config.ResourceProfile{ProfileID: "b", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
		},
	}
	runtimes := map[string]runtime.Runtime{
		"model-a": runtime.NewClient(context.Background(), cfg.Models["model-a"], cfg, runtime.ClientOptions{}),
		"model-b": runtime.NewClient(context.Background(), cfg.Models["model-b"], cfg, runtime.ClientOptions{}),
	}
	rt := router.NewExclusive(cfg, runtimes, nil)
	if err := rt.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
		for _, r := range runtimes {
			if c, ok := r.(*runtime.Client); ok {
				c.Shutdown()
			}
		}
	})
	return New(cfg, rt, runtimes).Handler()
}

func TestJSONRoutingAndInternalLoad(t *testing.T) {
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
	h := newTestStack(t, a, b)

	payload := `{"model":"alias-a","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/infer?model=alias-a", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
	if a.LastPath() != "/v1/audio/transcriptions" {
		t.Fatalf("path=%s", a.LastPath())
	}
	if string(a.LastBody()) != payload || a.LastContentType() != "application/json" {
		t.Fatalf("body=%s type=%s", a.LastBody(), a.LastContentType())
	}
	if w.Header().Get("Content-Type") == "" || !bytes.Contains(w.Body.Bytes(), []byte("mock")) {
		t.Fatalf("upstream response headers=%v body=%s", w.Header(), w.Body.Bytes())
	}
	if a.Counts().LoadStarts != 1 {
		t.Fatalf("expected internal load, counts=%+v", a.Counts())
	}
	if b.Counts().LoadStarts != 0 {
		t.Fatal("B should stay unloaded")
	}
}

func TestUnknownModel404(t *testing.T) {
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
	h := newTestStack(t, a, b)
	req := httptest.NewRequest(http.MethodPost, "/infer?model=nope", strings.NewReader(`{"model":"nope"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestMultipartRouting(t *testing.T) {
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
	h := newTestStack(t, a, b)

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("model", "model-a")
	fw, _ := mw.CreateFormFile("file", "x.wav")
	_, _ = fw.Write([]byte("RIFF"))
	ct := mw.FormDataContentType()
	_ = mw.Close()
	raw := append([]byte(nil), body.Bytes()...)
	req := httptest.NewRequest(http.MethodPost, "/infer?model=model-a&keep=1", bytes.NewReader(raw))
	req.Header.Set("Content-Type", ct)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
	if a.LastPath() != "/v1/audio/transcriptions?keep=1" {
		t.Fatalf("path=%s", a.LastPath())
	}
	if !bytes.Equal(a.LastBody(), raw) || !strings.Contains(string(a.LastBody()), `name="model"`) {
		t.Fatal("multipart body was rewritten")
	}
}

func TestModelsHidesUnlistedAndDistinguishesState(t *testing.T) {
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
	h := newTestStack(t, a, b)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d", w.Code)
	}
	raw, _ := io.ReadAll(w.Body)
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	data := payload["data"].([]any)
	if len(data) != 1 {
		t.Fatalf("unlisted B should be hidden, data=%s", raw)
	}
	item := data[0].(map[string]any)
	st := item["status"].(map[string]any)
	if st["state"] == "unloaded" {
		t.Fatal("local stopped must not be reported as unloaded")
	}
	if st["state"] != "stopped" || st["ready"] != false || st["residency"] != "not_resident" || st["remote_state"] != "unloaded" {
		t.Fatalf("status=%v", st)
	}
}

func TestHealthIsProcessLiveness(t *testing.T) {
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
	h := newTestStack(t, a, b)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d", w.Code)
	}
}

func TestAlignStripsModelQuery(t *testing.T) {
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
	h := newTestStack(t, a, b)
	body := []byte(`{"audio":"x"}`)
	req := httptest.NewRequest(http.MethodPost, "/infer?model=model-b&x=1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
	if b.LastPath() != "/align?x=1" {
		t.Fatalf("backend path=%s", b.LastPath())
	}
	if a.Counts().LoadStarts != 0 {
		t.Fatal("A should stay unloaded when B fits")
	}
}

func TestBodyLimit(t *testing.T) {
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
	h := newTestStack(t, a, b)
	req := httptest.NewRequest(http.MethodPost, "/infer?model=alias-a", bytes.NewReader([]byte(`{"model":"alias-a"}`)))
	req.ContentLength = 32<<20 + 1
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
}

func TestUnsupportedPath404(t *testing.T) {
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
	h := newTestStack(t, a, b)
	for _, path := range []string{
		"/v1/other",
		"/v1/chat/completions",
		"/v1/completions",
		"/v1/embeddings",
		"/v1/audio/transcriptions",
		"/align",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 404 {
			t.Fatalf("%s code=%d", path, w.Code)
		}
	}
}

func TestModelsReportsQueueReservationAndError(t *testing.T) {
	gate := testkit.NewBarrier()
	a, err := mock.New("A", mock.WithInferGate(gate))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := mock.New("B")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	cfg := &config.Config{
		MaxQueueSize:       16,
		QueueTimeout:       time.Minute,
		StatusTimeout:      2 * time.Second,
		UnloadTimeout:      2 * time.Second,
		HealthCheckTimeout: 5 * time.Second,
		Models: map[string]config.Model{
			"model-a": {
				ID: "model-a", Name: "A", BaseURL: a.URL(),
				ConcurrencyLimit: 1, HealthCheckTimeout: 5 * time.Second, UnloadTimeout: 2 * time.Second,
				InferencePath:    "/v1/chat/completions",
				MaxBodyBytes:    32 << 20,
				ResourceProfile: config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
			"model-b": {
				ID: "model-b", Name: "B", BaseURL: b.URL(),
				ConcurrencyLimit: 1, HealthCheckTimeout: 5 * time.Second, UnloadTimeout: 2 * time.Second,
				InferencePath:    "/align",
				MaxBodyBytes:    32 << 20,
				ResourceProfile: config.ResourceProfile{ProfileID: "b", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
		},
	}
	runtimes := map[string]runtime.Runtime{
		"model-a": runtime.NewClient(context.Background(), cfg.Models["model-a"], cfg, runtime.ClientOptions{}),
		"model-b": runtime.NewClient(context.Background(), cfg.Models["model-b"], cfg, runtime.ClientOptions{}),
	}
	rt := router.NewExclusive(cfg, runtimes, nil)
	if err := rt.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		gate.Release()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
		for _, item := range runtimes {
			if c, ok := item.(*runtime.Client); ok {
				c.Shutdown()
			}
		}
	})
	h := New(cfg, rt, runtimes).Handler()

	go func() {
		req := httptest.NewRequest(http.MethodPost, "/infer?model=model-a", strings.NewReader(`{"model":"model-a","messages":[]}`))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := gate.WaitStarted(ctx); err != nil {
		t.Fatal(err)
	}
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/infer?model=model-a", strings.NewReader(`{"model":"model-a","messages":[]}`))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(httptest.NewRecorder(), req)
	}()
	b.ForceFailed(mock.ResidencyNotResident)

	deadline := time.Now().Add(5 * time.Second)
	var payload map[string]any
	var raw []byte
	for {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		raw = append([]byte(nil), w.Body.Bytes()...)
		payload = map[string]any{}
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["queue_depth"] == float64(1) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue not visible: %s", raw)
		}
		time.Sleep(20 * time.Millisecond)
	}
	gpu, ok := payload["gpu"].(map[string]any)
	if !ok {
		t.Fatalf("gpu=%v", payload["gpu"])
	}
	if _, ok := gpu["fresh"]; !ok {
		t.Fatalf("gpu=%v", gpu)
	}
	data := payload["data"].([]any)
	var sawReserved, sawError bool
	for _, item := range data {
		row := item.(map[string]any)
		st := row["status"].(map[string]any)
		switch row["id"] {
		case "model-a":
			if st["reserved_bytes"] != float64(10) {
				t.Fatalf("reserved=%v body=%s", st["reserved_bytes"], raw)
			}
			if st["reason"] != "ready" {
				t.Fatalf("reason=%v", st["reason"])
			}
			sawReserved = true
		case "model-b":
			last, _ := st["last_error"].(map[string]any)
			if last["code"] != "LOAD_FAILED" {
				t.Fatalf("last_error=%v", st["last_error"])
			}
			sawError = true
		}
	}
	if !sawReserved || !sawError {
		t.Fatalf("missing fields: %s", raw)
	}
}

func TestInferQueryModelOnly(t *testing.T) {
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
	h := newTestStack(t, a, b)
	for _, path := range []string{"/infer", "/infer?model=", "/infer?model=%20"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"model":"alias-a"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s code=%d", path, w.Code)
		}
	}
	if a.Counts().InferStarts != 0 {
		t.Fatal("body model was used for routing")
	}
}

func TestInferPreservesFAPayloadAndChunkedBody(t *testing.T) {
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
	h := newTestStack(t, a, b)
	asr := []byte(`{"chunks":[{"index":0,"text":"안녕","start_sample":0,"end_sample":1}]}`)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "canonical.wav")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write([]byte("RIFF")); err != nil {
		t.Fatal(err)
	}
	if err := mw.WriteField("payload", string(asr)); err != nil {
		t.Fatal(err)
	}
	ct := mw.FormDataContentType()
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	raw := append([]byte(nil), buf.Bytes()...)
	req := httptest.NewRequest(http.MethodPost, "/infer?model=model-b", bytes.NewReader(raw))
	req.Header.Set("Content-Type", ct)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
	if b.LastPath() != "/align" || !bytes.Equal(b.LastBody(), raw) || !bytes.Contains(b.LastBody(), asr) {
		t.Fatalf("path=%s body=%s", b.LastPath(), b.LastBody())
	}
	if !strings.Contains(b.LastContentType(), "multipart/form-data") {
		t.Fatalf("type=%s", b.LastContentType())
	}

	chunk := []byte(`{"model":"in-body","n":1}`)
	creq := httptest.NewRequest(http.MethodPost, "/infer?model=alias-a", bytes.NewReader(chunk))
	creq.ContentLength = -1
	creq.TransferEncoding = []string{"chunked"}
	creq.Header.Set("Content-Type", "application/json")
	cw := httptest.NewRecorder()
	h.ServeHTTP(cw, creq)
	if cw.Code != 200 || !bytes.Equal(a.LastBody(), chunk) || a.LastPath() != "/v1/audio/transcriptions" {
		t.Fatalf("chunk code=%d path=%s body=%s", cw.Code, a.LastPath(), a.LastBody())
	}
}

func TestInferStreamsMaxBytes(t *testing.T) {
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
	cfg := &config.Config{
		MaxQueueSize: 4, QueueTimeout: time.Minute, StatusTimeout: time.Second,
		Models: map[string]config.Model{
			"model-a": {
				ID: "model-a", BaseURL: a.URL(), ConcurrencyLimit: 1,
				HealthCheckTimeout: time.Second, UnloadTimeout: time.Second,
				InferencePath: "/v1/audio/transcriptions", MaxBodyBytes: 16,
				ResourceProfile: config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
		},
	}
	c := runtime.NewClient(context.Background(), cfg.Models["model-a"], cfg, runtime.ClientOptions{})
	rt := router.NewExclusive(cfg, map[string]runtime.Runtime{"model-a": c}, nil)
	if err := rt.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = rt.Shutdown(ctx)
		c.Shutdown()
	})
	h := New(cfg, rt, map[string]runtime.Runtime{"model-a": c}).Handler()
	req := httptest.NewRequest(http.MethodPost, "/infer?model=model-a", strings.NewReader(strings.Repeat("x", 64)))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
	}
}

func TestInferCancelBeforeAdmit(t *testing.T) {
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
	h := newTestStack(t, a, b)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/infer?model=alias-a", strings.NewReader(`{"model":"alias-a"}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if a.Counts().InferStarts != 0 || a.Counts().LoadStarts != 0 {
		t.Fatalf("cancelled request reached the runtime: %+v", a.Counts())
	}
}
