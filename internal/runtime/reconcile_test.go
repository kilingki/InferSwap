package runtime

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/runtime/mock"
)

func TestReconcileKeepsExistingReady(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	resp, err := http.Post(s.URL()+"/control/load", "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("load=%d", resp.StatusCode)
	}
	rec := &recCommander{fn: func(context.Context, []string, string) error {
		t.Fatal("reconcile must not prepare")
		return nil
	}}
	model := testModel(s.URL())
	model.Prepare = &config.Prepare{Argv: []string{"/abs/prepare"}}
	c := newClient(t, s, model, ClientOptions{Commander: rec})
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateReady {
		t.Fatalf("state=%s", c.State())
	}
	if rec.Runs() != 0 {
		t.Fatal("prepare during reconcile")
	}
}

func TestReconcileDownEndpointDoesNotPrepare(t *testing.T) {
	s, err := mock.New("A", mock.WithEndpointDown())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rec := &recCommander{}
	model := testModel(s.URL())
	model.Prepare = &config.Prepare{Argv: []string{"/abs/prepare"}}
	c := newClient(t, s, model, ClientOptions{Commander: rec})
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rec.Runs() != 0 {
		t.Fatal("prepare on missing endpoint during reconcile")
	}
	if c.State() == StateStopped || c.State() == StateReady {
		t.Fatalf("missing endpoint should stay unknown, state=%s", c.State())
	}
}
