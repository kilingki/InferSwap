package runtime

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/kilingki/InferSwap/internal/runtime/mock"
)

func TestClassifyRecoveryMatrix(t *testing.T) {
	cases := []struct {
		name string
		st   Status
		want RecoveryClass
	}{
		{"ready idle", Status{State: WireReady, Residency: ResidencyResident, ActiveRequests: 0}, RecReadyIdle},
		{"ready busy", Status{State: WireReady, Residency: ResidencyResident, ActiveRequests: 2}, RecReadyBusy},
		{"unloaded", Status{State: WireUnloaded, Residency: ResidencyNotResident}, RecUnloadedIdle},
		{"failed clear", Status{State: WireFailed, Residency: ResidencyNotResident}, RecFailedNotResidentIdle},
		{"failed busy", Status{State: WireFailed, Residency: ResidencyNotResident, ActiveRequests: 1}, RecFailedNotResidentBusy},
		{"failed resident", Status{State: WireFailed, Residency: ResidencyResident}, RecFailedOccupied},
		{"failed unknown", Status{State: WireFailed, Residency: ResidencyUnknown}, RecFailedOccupied},
		{"loading", Status{State: WireLoading, Residency: ResidencyUnknown}, RecInProgress},
		{"unloading", Status{State: WireUnloading, Residency: ResidencyResident}, RecInProgress},
		{"ready not resident", Status{State: WireReady, Residency: ResidencyNotResident}, RecInvalid},
		{"unloaded resident", Status{State: WireUnloaded, Residency: ResidencyResident}, RecInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyRecovery(tc.st); got != tc.want {
				t.Fatalf("class=%v want %v", got, tc.want)
			}
		})
	}
}

func TestClassifyStatusErrKinds(t *testing.T) {
	if ClassifyStatusErr(nil) != StatusErrNone {
		t.Fatal("nil")
	}
	if ClassifyStatusErr(context.DeadlineExceeded) != StatusErrOther {
		t.Fatal("timeout must not be endpoint down")
	}
	if ClassifyStatusErr(&net.OpError{Err: errors.New("connection refused")}) != StatusErrEndpointDown {
		t.Fatal("refused")
	}
	if ClassifyStatusErr(errors.New("status: HTTP 500")) != StatusErrOther {
		t.Fatal("http")
	}
}

func TestApplyReadyAndFailedNotResident(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.ForceFailed(mock.ResidencyNotResident)
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateFailed {
		t.Fatalf("state=%s", c.State())
	}
	before := c.Generation()
	ack := apply(t, c, ApplyCmd{
		Gen:   before,
		Class: RecFailedNotResidentIdle,
		HasStatus: true,
		Status: Status{
			State:     WireFailed,
			Residency: ResidencyNotResident,
			LastError: &LastError{Code: "LOAD_FAILED", Message: "injected"},
		},
	})
	if ack != ApplyApplied {
		t.Fatalf("ack=%v", ack)
	}
	if c.State() != StateStopped || c.StateReason() != "recovered_remote_failed" {
		t.Fatalf("state=%s reason=%s", c.State(), c.StateReason())
	}
	diag := c.RemoteDiag()
	if !diag.Known || diag.Status.State != WireFailed || diag.Status.LastError == nil || diag.Status.LastError.Code != "LOAD_FAILED" {
		t.Fatalf("remote=%+v", diag)
	}
	if c.Generation() == before {
		t.Fatal("applied result must advance generation")
	}
	stale := apply(t, c, ApplyCmd{Gen: before, Class: RecReadyIdle, HasStatus: true, Status: readyIdle()})
	if stale != ApplyStale || c.State() != StateStopped {
		t.Fatalf("stale ack=%v state=%s", stale, c.State())
	}
}

func TestApplyDoesNotStaleItself(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	ack := apply(t, c, ApplyCmd{Gen: c.Generation(), Class: RecReadyIdle, HasStatus: true, Status: readyIdle()})
	if ack != ApplyApplied || c.State() != StateReady {
		t.Fatalf("ack=%v state=%s", ack, c.State())
	}
}

func TestApplyBusyDoesNotBumpOrFinishWaiter(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	c.SetLoadGate(func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	done := make(chan error, 1)
	go func() { done <- c.EnsureReady(context.Background(), 2*time.Second) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("load gate")
	}
	gen := c.Generation()
	ack := apply(t, c, ApplyCmd{Gen: gen, Class: RecReadyIdle, HasStatus: true, Status: readyIdle()})
	if ack != ApplyBusy {
		t.Fatalf("ack=%v", ack)
	}
	if c.Generation() != gen {
		t.Fatal("busy must not bump generation")
	}
	if c.State() == StateReady {
		t.Fatal("busy apply changed state")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c.State() != StateReady {
		t.Fatalf("waiter finished in %s", c.State())
	}
}

func TestIdleStopBumpsGeneration(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := c.Generation()
	if err := c.Stop(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if c.Generation() == before {
		t.Fatal("idle stop did not bump generation")
	}
	if c.State() != StateStopped {
		t.Fatalf("state=%s", c.State())
	}
	ack := apply(t, c, ApplyCmd{Gen: before, Class: RecReadyIdle, HasStatus: true, Status: readyIdle()})
	if ack != ApplyStale || c.State() != StateStopped {
		t.Fatalf("ack=%v state=%s", ack, c.State())
	}
}

func TestReconcileUnloadedDoesNotUseRecoveryApply(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateStopped || c.StateReason() != "reconcile" {
		t.Fatalf("state=%s reason=%s", c.State(), c.StateReason())
	}
}

func TestStaleApplyDoesNotClobberReady(t *testing.T) {
	s, err := mock.New("A")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := newClient(t, s, testModel(s.URL()), ClientOptions{})
	if apply(t, c, ApplyCmd{Gen: c.Generation(), Class: RecReadyIdle, HasStatus: true, Status: readyIdle()}) != ApplyApplied {
		t.Fatal("apply")
	}
	stopGen := make(chan uint64, 1)
	errCh := make(chan error, 1)
	go func() {
		stopGen <- c.Generation()
		errCh <- c.Stop(context.Background(), time.Second)
	}()
	var stale ApplyCode
	select {
	case g := <-stopGen:
		stale = apply(t, c, ApplyCmd{
			Gen:       g,
			Class:     RecFailedNotResidentIdle,
			HasStatus: true,
			Status:    Status{State: WireFailed, Residency: ResidencyNotResident},
		})
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not start")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if stale == ApplyApplied {
		t.Fatal("stale lifecycle result was applied")
	}
	if c.State() != StateStopped {
		t.Fatalf("state=%s", c.State())
	}
}

func apply(t *testing.T, c *Client, cmd ApplyCmd) ApplyCode {
	t.Helper()
	ack := make(chan ApplyCode, 1)
	select {
	case c.RecoveryReqs() <- ApplyReq{Cmd: cmd, Ack: ack}:
	case <-time.After(2 * time.Second):
		t.Fatal("apply send")
	}
	select {
	case code := <-ack:
		return code
	case <-time.After(2 * time.Second):
		t.Fatal("apply ack")
		return ApplyStale
	}
}

func readyIdle() Status {
	return Status{State: WireReady, Residency: ResidencyResident}
}
