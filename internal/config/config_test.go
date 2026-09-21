package config

import (
	"strings"
	"testing"
	"time"
)

const validYAML = `
listen: ":8080"
logLevel: info
healthCheckTimeout: 120
unloadTimeout: 30
queueTimeout: 180
prepareTimeout: 60
statusTimeout: 5
drainTimeout: 180
shutdownTimeout: 60
maxQueueSize: 256
preload: ["qwen3.8-27b"]

models:
  qwen3.8-27b:
    baseURL: "http://127.0.0.1:8000"
    name: "Qwen3.8 27B"
    aliases: [qwen]
    concurrencyLimit: 1
    unlisted: false
    prepare:
      argv: ["/absolute/path/model-project/prepare-inferswap"]
    timeouts:
      prepareTimeout: 90
      healthCheckTimeout: 120
      unloadTimeout: 30
  qwen-asr:
    baseURL: "http://127.0.0.1:8001"
    name: "Qwen ASR"
`

func TestLoadValid(t *testing.T) {
	cfg, err := Load([]byte(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	id, ok := cfg.Resolve("qwen")
	if !ok || id != "qwen3.8-27b" {
		t.Fatalf("Resolve(qwen)=%q ok=%v", id, ok)
	}
	m, ok := cfg.Model("qwen")
	if !ok {
		t.Fatal("model qwen")
	}
	if m.PrepareTimeout != 90*time.Second {
		t.Fatalf("override prepareTimeout=%s", m.PrepareTimeout)
	}
	asr, ok := cfg.Model("qwen-asr")
	if !ok {
		t.Fatal("qwen-asr")
	}
	if asr.PrepareTimeout != 60*time.Second {
		t.Fatalf("inherited prepareTimeout=%s", asr.PrepareTimeout)
	}
	if asr.Prepare != nil {
		t.Fatal("prepare should be optional")
	}
	if m.Prepare == nil || m.Prepare.Argv[0] != "/absolute/path/model-project/prepare-inferswap" {
		t.Fatalf("prepare=%v", m.Prepare)
	}
}

func TestRejectMissingBaseURL(t *testing.T) {
	mustReject(t, `
models:
  a:
    name: A
`, "baseURL")
}

func TestRejectAliasCollision(t *testing.T) {
	mustReject(t, `
models:
  a:
    baseURL: "http://127.0.0.1:1"
    aliases: [b]
  b:
    baseURL: "http://127.0.0.1:2"
`, "collides")
	mustReject(t, `
models:
  a:
    baseURL: "http://127.0.0.1:1"
    aliases: [x]
  b:
    baseURL: "http://127.0.0.1:2"
    aliases: [x]
`, "collides")
}

func TestRejectMissingPreload(t *testing.T) {
	mustReject(t, `
preload: ["missing"]
models:
  a:
    baseURL: "http://127.0.0.1:1"
`, "preload")
}

func TestRejectNonPositiveLimits(t *testing.T) {
	mustReject(t, `
maxQueueSize: 0
models:
  a:
    baseURL: "http://127.0.0.1:1"
`, "maxQueueSize")
	mustReject(t, `
models:
  a:
    baseURL: "http://127.0.0.1:1"
    concurrencyLimit: 0
`, "concurrencyLimit")
	mustReject(t, `
queueTimeout: 0
models:
  a:
    baseURL: "http://127.0.0.1:1"
`, "queueTimeout")
}

func TestRejectPrepare(t *testing.T) {
	mustReject(t, `
models:
  a:
    baseURL: "http://127.0.0.1:1"
    prepare:
      argv: []
`, "argv")
	mustReject(t, `
models:
  a:
    baseURL: "http://127.0.0.1:1"
    prepare:
      argv: ["relative/prepare"]
`, "absolute")
}

func TestRejectForbiddenFields(t *testing.T) {
	mustReject(t, `
macros:
  x: y
models:
  a:
    baseURL: "http://127.0.0.1:1"
`, "macros")
	mustReject(t, `
models:
  a:
    baseURL: "http://127.0.0.1:1"
    cmd: "echo"
`, "cmd")
	mustReject(t, `
models:
  a:
    baseURL: "http://127.0.0.1:1"
    timeouts:
      queueTimeout: 1
`, "queueTimeout")
}

func TestLoadExampleFile(t *testing.T) {
	cfg, err := LoadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Resolve("qwen"); !ok {
		t.Fatal("example alias qwen")
	}
}

func TestResolveCanonical(t *testing.T) {
	cfg, err := Load([]byte(`
models:
  a:
    baseURL: "http://127.0.0.1:1"
    aliases: [alpha]
`))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := cfg.Resolve("a")
	if !ok || got != "a" {
		t.Fatalf("canonical Resolve=%q", got)
	}
	got, ok = cfg.Resolve("alpha")
	if !ok || got != "a" {
		t.Fatalf("alias Resolve=%q", got)
	}
	if _, ok := cfg.Resolve("missing"); ok {
		t.Fatal("missing should not resolve")
	}
}

func mustReject(t *testing.T, yml, substr string) {
	t.Helper()
	_, err := Load([]byte(yml))
	if err == nil {
		t.Fatalf("expected error containing %q", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("err=%q want substring %q", err, substr)
	}
}
