package proxy

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestQueryModelDoesNotReadBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/completions?model=qwen", errReader{})
	r.ContentLength = 4
	got, err := ExtractModelLimited(r, 32)
	if err != nil {
		t.Fatal(err)
	}
	if got != "qwen" {
		t.Fatalf("model=%s", got)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (errReader) Close() error             { return nil }

func TestSpoolPreservesBodyOver32MiB(t *testing.T) {
	pad := bytes.Repeat([]byte("a"), (32<<20)+8)
	raw := append([]byte(`{"model":"qwen","prompt":"`), pad...)
	raw = append(raw, []byte(`"}`)...)
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", bytes.NewReader(raw))
	r.Header.Set("Content-Type", "application/json")
	r.ContentLength = -1
	got, err := ExtractModelLimited(r, int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if got != "qwen" {
		t.Fatalf("model=%s", got)
	}
	restored, err := ioReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, raw) {
		t.Fatalf("len got=%d want=%d", len(restored), len(raw))
	}
	path := r.Body.(*spoolFile).path
	if err := r.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("temp file remains: %v", err)
	}
}

func TestChunkedOverLimitRemovesSpool(t *testing.T) {
	before := spoolNames()
	r := httptest.NewRequest(http.MethodPost, "/v1/completions", bytes.NewReader([]byte(`{"model":"qwen","prompt":"abcdef"}`)))
	r.Header.Set("Content-Type", "application/json")
	r.ContentLength = -1
	_, err := ExtractModelLimited(r, 8)
	if err != ErrBodyTooLarge {
		t.Fatalf("err=%v", err)
	}
	for name := range spoolNames() {
		if !before[name] {
			t.Fatalf("temp file left behind: %s", name)
		}
	}
}

func spoolNames() map[string]bool {
	matches, _ := filepath.Glob(filepath.Join(os.TempDir(), "inferswap-body-*"))
	out := map[string]bool{}
	for _, name := range matches {
		out[name] = true
	}
	return out
}

func ioReadAll(r *http.Request) ([]byte, error) {
	var b bytes.Buffer
	_, err := b.ReadFrom(r.Body)
	return b.Bytes(), err
}
