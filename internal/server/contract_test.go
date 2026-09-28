package server

import (
	"bytes"
	"context"
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

// TestIntegrationScriptContract checks the request shape used by
// tests/test_asr_fa_swap.py. The test posts only /infer. It does not call
// /control/load or /control/unload.
func TestIntegrationScriptContract(t *testing.T) {
	asr, err := mock.New("ASR")
	if err != nil {
		t.Fatal(err)
	}
	defer asr.Close()
	fa, err := mock.New("FA")
	if err != nil {
		t.Fatal(err)
	}
	defer fa.Close()
	asr.ForceReady()
	fa.ForceReady()

	cfg := &config.Config{
		MaxQueueSize:       4,
		QueueTimeout:       time.Minute,
		StatusTimeout:      time.Second,
		UnloadTimeout:      time.Second,
		HealthCheckTimeout: time.Second,
		Models: map[string]config.Model{
			"qwen-asr": {
				ID: "qwen-asr", Name: "Qwen ASR", BaseURL: asr.URL(), Aliases: []string{"qwen3-asr"},
				ConcurrencyLimit: 1, HealthCheckTimeout: time.Second, UnloadTimeout: time.Second,
				InferencePath: "/v1/audio/transcriptions", MaxBodyBytes: 1 << 20,
				ResourceProfile: config.ResourceProfile{ProfileID: "asr", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
			"qwen-fa": {
				ID: "qwen-fa", Name: "Qwen FA", BaseURL: fa.URL(),
				ConcurrencyLimit: 1, HealthCheckTimeout: time.Second, UnloadTimeout: time.Second,
				InferencePath: "/align", MaxBodyBytes: 1 << 20,
				ResourceProfile: config.ResourceProfile{ProfileID: "fa", LoadPeakBytes: 10, InferencePeakBytes: 10, MaxConcurrency: 1},
			},
		},
	}
	runtimes := map[string]runtime.Runtime{
		"qwen-asr": runtime.NewClient(context.Background(), cfg.Models["qwen-asr"], cfg, runtime.ClientOptions{}),
		"qwen-fa":  runtime.NewClient(context.Background(), cfg.Models["qwen-fa"], cfg, runtime.ClientOptions{}),
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
	h := New(cfg, rt, runtimes).Handler()

	asrJSON := []byte(`{"audio":{"sample_rate":16000,"channels":1,"num_samples":1},"chunks":[{"index":0,"text":"안녕","start_sample":0,"end_sample":1}]}`)
	var asrBody bytes.Buffer
	aw := multipart.NewWriter(&asrBody)
	fw, err := aw.CreateFormFile("file", "canonical.wav")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write([]byte("RIFFpcm")); err != nil {
		t.Fatal(err)
	}
	if err := aw.WriteField("response_format", "verbose_json"); err != nil {
		t.Fatal(err)
	}
	if err := aw.WriteField("include_chunks", "true"); err != nil {
		t.Fatal(err)
	}
	if err := aw.WriteField("model", "qwen-fa"); err != nil {
		t.Fatal(err)
	}
	asrType := aw.FormDataContentType()
	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}
	asrRaw := append([]byte(nil), asrBody.Bytes()...)

	req := httptest.NewRequest(http.MethodPost, "/infer?model=qwen-asr&keep=1", bytes.NewReader(asrRaw))
	req.Header.Set("Content-Type", asrType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("asr code=%d body=%s", w.Code, w.Body.Bytes())
	}
	if asr.LastPath() != "/v1/audio/transcriptions?keep=1" {
		t.Fatalf("asr path=%s", asr.LastPath())
	}
	if strings.Contains(asr.LastPath(), "model=") {
		t.Fatalf("model query was forwarded: %s", asr.LastPath())
	}
	if !bytes.Equal(asr.LastBody(), asrRaw) || !strings.Contains(asr.LastContentType(), "multipart/form-data") {
		t.Fatalf("asr body changed type=%s", asr.LastContentType())
	}
	if !bytes.Contains(asr.LastBody(), []byte("verbose_json")) || !bytes.Contains(asr.LastBody(), []byte("include_chunks")) || !bytes.Contains(asr.LastBody(), []byte("qwen-fa")) {
		t.Fatal("asr multipart fields were rewritten")
	}

	var faBody bytes.Buffer
	fwri := multipart.NewWriter(&faBody)
	ff, err := fwri.CreateFormFile("file", "canonical.wav")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ff.Write([]byte("RIFFpcm")); err != nil {
		t.Fatal(err)
	}
	if err := fwri.WriteField("payload", string(asrJSON)); err != nil {
		t.Fatal(err)
	}
	faType := fwri.FormDataContentType()
	if err := fwri.Close(); err != nil {
		t.Fatal(err)
	}
	faRaw := append([]byte(nil), faBody.Bytes()...)
	freq := httptest.NewRequest(http.MethodPost, "/infer?model=qwen-fa", bytes.NewReader(faRaw))
	freq.Header.Set("Content-Type", faType)
	fwrec := httptest.NewRecorder()
	h.ServeHTTP(fwrec, freq)
	if fwrec.Code != http.StatusOK {
		t.Fatalf("fa code=%d body=%s", fwrec.Code, fwrec.Body.Bytes())
	}
	if fa.LastPath() != "/align" {
		t.Fatalf("fa path=%s", fa.LastPath())
	}
	if !bytes.Equal(fa.LastBody(), faRaw) || !bytes.Contains(fa.LastBody(), asrJSON) {
		t.Fatal("fa payload was not the original ASR JSON text")
	}
	if asr.Counts().LoadStarts != 0 || asr.Counts().UnloadStarts != 0 || fa.Counts().LoadStarts != 0 || fa.Counts().UnloadStarts != 0 {
		t.Fatalf("contract test drove control load/unload asr=%+v fa=%+v", asr.Counts(), fa.Counts())
	}
}
