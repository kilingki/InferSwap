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
			},
			"model-b": {
				ID: "model-b", Name: "B", BaseURL: b.URL(),
				ConcurrencyLimit: 1, HealthCheckTimeout: 5 * time.Second, UnloadTimeout: 2 * time.Second, Unlisted: true,
			},
		},
	}
	runtimes := map[string]runtime.Runtime{
		"model-a": runtime.NewClient(context.Background(), cfg.Models["model-a"], cfg, runtime.ClientOptions{}),
		"model-b": runtime.NewClient(context.Background(), cfg.Models["model-b"], cfg, runtime.ClientOptions{}),
	}
	rt := router.NewExclusive(cfg, runtimes, nil)
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
	if st["state"] == "unloaded" && st["ready"] == false {
		t.Fatal("ready=false must not be collapsed to unloaded")
	}
	if st["state"] != "unknown" && st["ready"] != nil {
		// initial client state is unknown; ready should be null not false-as-unloaded
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
