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
				InferencePaths:  []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/audio/transcriptions", "/align"},
				MaxBodyBytes:    32 << 20,
				ResourceProfile: config.ResourceProfile{ProfileID: "a", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
			"model-b": {
				ID: "model-b", Name: "B", BaseURL: b.URL(),
				ConcurrencyLimit: 1, HealthCheckTimeout: 5 * time.Second, UnloadTimeout: 2 * time.Second, Unlisted: true,
				InferencePaths:  []string{"/v1/chat/completions", "/align"},
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

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"alias-a","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
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
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"model":"nope"}`))
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
	req := httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", bytes.NewReader(body.Bytes()))
	req.Header.Set("Content-Type", ct)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.Bytes())
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
	if st["state"] != "stopped" || st["ready"] != false || st["residency"] != "not_resident" {
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
	req := httptest.NewRequest(http.MethodPost, "/align?model=model-b&x=1", bytes.NewReader(body))
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
	req := httptest.NewRequest(http.MethodPost, "/v1/completions?model=alias-a", bytes.NewReader([]byte(`{"model":"alias-a"}`)))
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
	req := httptest.NewRequest(http.MethodPost, "/v1/other", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("code=%d", w.Code)
	}
}
