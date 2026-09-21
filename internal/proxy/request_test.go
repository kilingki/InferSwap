package proxy

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExtractJSONModelAndRestoreBody(t *testing.T) {
	raw := `{"model":"qwen","prompt":"hi"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(raw))
	r.Header.Set("Content-Type", "application/json")
	got, err := ExtractModel(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != "qwen" {
		t.Fatalf("model=%s", got)
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		t.Fatal(err)
	}
	if buf.String() != raw {
		t.Fatalf("body not restored: %s", buf.String())
	}
}

func TestExtractMultipartPreservesBoundary(t *testing.T) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := w.WriteField("model", "qwen-asr"); err != nil {
		t.Fatal(err)
	}
	fw, err := w.CreateFormFile("file", "a.wav")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fw.Write([]byte("RIFF")); err != nil {
		t.Fatal(err)
	}
	ct := w.FormDataContentType()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	orig := body.Bytes()
	r := httptest.NewRequest(http.MethodPost, "/v1/audio/transcriptions", bytes.NewReader(orig))
	r.Header.Set("Content-Type", ct)
	got, err := ExtractModel(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != "qwen-asr" {
		t.Fatalf("model=%s", got)
	}
	restored, _ := ioReadAll(r)
	if !bytes.Equal(restored, orig) {
		t.Fatalf("multipart bytes changed")
	}
	if !strings.Contains(ct, "boundary=") {
		t.Fatal("missing boundary")
	}
}

func TestExtractMissingModel(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"x"}`))
	r.Header.Set("Content-Type", "application/json")
	_, err := ExtractModel(r)
	if err != ErrNoModel {
		t.Fatalf("err=%v", err)
	}
}

func ioReadAll(r *http.Request) ([]byte, error) {
	var b bytes.Buffer
	_, err := b.ReadFrom(r.Body)
	return b.Bytes(), err
}
