package scheduler

import (
	"testing"

	"github.com/kilingki/InferSwap/internal/runtime"
)

type hostFake struct {
	fakeEffects
	facts  RecoveryFacts
	probes []string
}

func (h *hostFake) RecoveryFacts() RecoveryFacts { return h.facts }

func (h *hostFake) BeginProbe(token uint64, model string) {
	h.probes = append(h.probes, model)
}

type countPlan struct {
	evicts  int
	decides int
	err     error
}

func (c *countPlan) EvictionFor(target string, running []string) []string {
	c.evicts++
	return nil
}

func (c *countPlan) Decide(target string, running []string) ([]string, error) {
	c.decides++
	if c.err != nil {
		return nil, c.err
	}
	return nil, nil
}

func TestRecoverPhaseSkipsDecideUntilProbesFinish(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateFailed,
		"B": runtime.StateFailed,
		"C": runtime.StateReady,
	})}
	host.facts = RecoveryFacts{States: map[string]runtime.State{
		"A": runtime.StateFailed,
		"B": runtime.StateFailed,
		"C": runtime.StateReady,
	}}
	plan := &countPlan{}
	s := NewFIFO(testCfg(), plan, host)
	s.OnRequest(req(1, "A"))
	host.states["A"] = runtime.StateReady
	host.facts.States["A"] = runtime.StateReady
	if plan.evicts != 0 || plan.decides != 0 {
		t.Fatalf("decide during recover evicts=%d decides=%d", plan.evicts, plan.decides)
	}
	if len(host.probes) != 1 || host.probes[0] != "A" {
		t.Fatalf("probes=%v", host.probes)
	}
	s.gateClosed["C"] = true
	s.OnStatus(StatusEvent{ModelID: "C", Gen: 1, Status: runtime.Status{ActiveRequests: 0}})
	if !s.gateClosed["C"] || plan.decides != 0 {
		t.Fatal("status during recover reopened dispatch")
	}
	s.NoteProbe(ProbeNote{Token: s.recoverToken, Model: "A", Mode: RecoverCurrent, Class: runtime.RecReadyIdle, Ack: runtime.ApplyApplied, Applied: true})
	if len(host.probes) != 1 {
		t.Fatalf("target ready must skip the rest, probes=%v", host.probes)
	}
	if plan.decides != 1 || len(host.grants) != 1 || !host.grants[0].serve {
		t.Fatalf("decides=%d grants=%v", plan.decides, host.grants)
	}
}

func TestProbeOrderThenSampleBeforeDecide(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateStopped,
		"B": runtime.StateFailed,
		"C": runtime.StateStopped,
	})}
	host.facts = RecoveryFacts{
		States:  map[string]runtime.State{"A": runtime.StateStopped, "B": runtime.StateFailed, "C": runtime.StateStopped},
		Markers: map[string]bool{"A": true},
	}
	plan := &countPlan{}
	s := NewFIFO(testCfg(), plan, host)
	s.unloadFail["A"] = true
	s.OnRequest(req(1, "C"))
	if len(host.probes) != 1 || host.probes[0] != "B" {
		t.Fatalf("probes=%v", host.probes)
	}
	host.states["B"] = runtime.StateReady
	host.facts.States["B"] = runtime.StateReady
	s.NoteProbe(ProbeNote{Token: s.recoverToken, Model: "B", Mode: RecoverCurrent, Class: runtime.RecReadyIdle, Ack: runtime.ApplyApplied, Applied: true, ProofGen: 3})
	if plan.decides != 0 || s.phase != phaseSample {
		t.Fatalf("phase=%d decides=%d", s.phase, plan.decides)
	}
	if len(host.probes) != 1 {
		t.Fatalf("A was status-probed: %v", host.probes)
	}
	delete(host.facts.Markers, "A")
	s.MarkerCleared("A")
	if s.unloadFail["A"] {
		t.Fatal("sticky survived the sample")
	}
	if plan.decides != 1 || len(host.loads) != 1 || host.loads[0] != "C" {
		t.Fatalf("decides=%d loads=%v", plan.decides, host.loads)
	}
}

func TestReadyHeadIsNotHeldForOtherRecovery(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateFailed,
		"B": runtime.StateReady,
	})}
	host.facts = RecoveryFacts{
		States:  map[string]runtime.State{"A": runtime.StateFailed, "B": runtime.StateReady},
		Markers: map[string]bool{"A": true},
	}
	plan := &countPlan{}
	s := NewFIFO(testCfg(), plan, host)
	s.OnRequest(req(1, "A"))
	if s.phase != phaseSample {
		t.Fatalf("phase=%d", s.phase)
	}
	s.OnRequest(req(2, "B"))
	if len(host.grants) != 0 {
		t.Fatal("ready request overtook the recover head")
	}
	s.OnCancel(req(1, "A"))
	if len(host.grants) != 1 || host.grants[0].model != "B" || !host.grants[0].serve {
		t.Fatalf("grants=%v", host.grants)
	}
	if len(host.probes) != 0 {
		t.Fatalf("ready head probed: %v", host.probes)
	}
}

func TestJoinCancelAndLateResults(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateUnknown,
		"B": runtime.StateReady,
	})}
	host.facts = RecoveryFacts{States: map[string]runtime.State{
		"A": runtime.StateUnknown,
		"B": runtime.StateReady,
	}}
	plan := &countPlan{}
	s := NewFIFO(testCfg(), plan, host)
	s.OnRequest(req(1, "A"))
	s.OnRequest(req(2, "A"))
	token := s.recoverToken
	s.OnCancel(req(1, "A"))
	if s.phase != phaseRecover || s.recoverOwner != 2 || s.recoverToken != token {
		t.Fatalf("phase=%d owner=%d token=%d", s.phase, s.recoverOwner, s.recoverToken)
	}
	if len(host.probes) != 1 {
		t.Fatalf("joined probe was restarted: %v", host.probes)
	}
	host.states["A"] = runtime.StateReady
	host.facts.States["A"] = runtime.StateReady
	s.NoteProbe(ProbeNote{Token: token, Model: "A", Mode: RecoverCurrent, Class: runtime.RecReadyIdle, Ack: runtime.ApplyApplied, Applied: true})
	if len(host.grants) != 1 || host.grants[0].model != "A" {
		t.Fatalf("grants=%v", host.grants)
	}

	host.states["A"] = runtime.StateFailed
	host.facts.States["A"] = runtime.StateFailed
	host.grants = nil
	host.probes = nil
	s.OnRequest(req(3, "A"))
	late := s.recoverToken
	s.OnCancel(req(3, "A"))
	s.OnRequest(req(4, "B"))
	if len(host.grants) != 1 || !host.grants[0].serve {
		t.Fatalf("ready grant=%v", host.grants)
	}
	s.NoteProbe(ProbeNote{Token: late, Model: "A", Mode: RecoverLate, Ack: runtime.ApplyStale})
	if len(host.grants) != 1 {
		t.Fatalf("late stale granted %v", host.grants)
	}
}

func TestLateResultDroppedOnceLoadingStarts(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateUnknown,
		"C": runtime.StateStopped,
	})}
	host.facts = RecoveryFacts{States: map[string]runtime.State{
		"A": runtime.StateUnknown,
		"C": runtime.StateStopped,
	}}
	plan := &countPlan{}
	s := NewFIFO(testCfg(), plan, host)
	s.OnRequest(req(1, "A"))
	token := s.recoverToken
	s.OnCancel(req(1, "A"))
	host.facts.States["A"] = runtime.StateStopped
	host.states["A"] = runtime.StateStopped
	s.OnRequest(req(2, "C"))
	if s.phase != phaseLoading || len(host.loads) != 1 {
		t.Fatalf("phase=%d loads=%v", s.phase, host.loads)
	}
	s.NoteProbe(ProbeNote{Token: token, Model: "A", Mode: RecoverDrop, Class: runtime.RecReadyIdle, Ack: runtime.ApplyApplied, Applied: true})
	if s.phase != phaseLoading {
		t.Fatalf("late result changed phase to %d", s.phase)
	}
	host.states["C"] = runtime.StateReady
	s.OnSwapDone(SwapDone{ModelID: "C"})
	if s.phase != phaseIdle || len(host.grants) != 1 {
		t.Fatalf("phase=%d grants=%v", s.phase, host.grants)
	}
}

func TestCurrentStaleIsResourcesAndBusyJoins(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateUnknown,
		"B": runtime.StateFailed,
	})}
	host.facts = RecoveryFacts{States: map[string]runtime.State{
		"A": runtime.StateUnknown,
		"B": runtime.StateFailed,
	}}
	plan := &countPlan{}
	s := NewFIFO(testCfg(), plan, host)
	s.OnRequest(req(1, "A"))
	s.NoteProbe(ProbeNote{Token: s.recoverToken, Model: "A", Mode: RecoverCurrent, Ack: runtime.ApplyStale})
	if len(host.grants) != 1 || host.grants[0].err != ErrResources {
		t.Fatalf("grants=%v", host.grants)
	}

	host.grants = nil
	host.probes = nil
	host.states["A"] = runtime.StateStarting
	host.facts.States["A"] = runtime.StateStarting
	host.facts.InFlight = map[string]bool{"A": true}
	s.OnRequest(req(2, "A"))
	if len(host.probes) != 0 || len(host.loads) != 1 {
		t.Fatalf("starting target probes=%v loads=%v", host.probes, host.loads)
	}

	host.loads = nil
	host.states["B"] = runtime.StateFailed
	host.facts.States = map[string]runtime.State{"A": runtime.StateFailed, "B": runtime.StateFailed}
	host.facts.InFlight = nil
	s.phase = phaseIdle
	s.OnRequest(req(3, "A"))
	tok := s.recoverToken
	host.states["A"] = runtime.StateStarting
	host.facts.States["A"] = runtime.StateStarting
	host.facts.States["B"] = runtime.StateStopped
	host.facts.InFlight = map[string]bool{"A": true}
	s.NoteProbe(ProbeNote{Token: tok, Model: "A", Mode: RecoverCurrent, Ack: runtime.ApplyBusy})
	if len(host.grants) != 0 || len(host.loads) != 1 || host.loads[0] != "A" {
		t.Fatalf("grants=%v loads=%v", host.grants, host.loads)
	}

	s.phase = phaseIdle
	s.swapTarget = ""
	host.loads = nil
	host.grants = nil
	host.probes = nil
	host.states["A"] = runtime.StateStopped
	host.states["B"] = runtime.StateFailed
	host.facts.States = map[string]runtime.State{"A": runtime.StateStopped, "B": runtime.StateFailed}
	s.OnRequest(req(4, "A"))
	if len(host.probes) != 1 || host.probes[0] != "B" {
		t.Fatalf("probes=%v", host.probes)
	}
	host.states["B"] = runtime.StateStarting
	s.NoteProbe(ProbeNote{Token: s.recoverToken, Model: "B", Mode: RecoverCurrent, Ack: runtime.ApplyBusy})
	if len(host.grants) != 1 || host.grants[0].err != ErrLifecyclePending || host.grants[0].model != "A" {
		t.Fatalf("grants=%v", host.grants)
	}
}

func TestRecoveryBackendClearsUnresolvedOnlyOnMatchingProof(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{"A": runtime.StateFailed})}
	host.facts = RecoveryFacts{States: map[string]runtime.State{"A": runtime.StateFailed}}
	s := NewFIFO(testCfg(), &countPlan{}, host)
	s.unresolved["A"] = 1
	s.dispatchGen["A"] = 4
	s.quietGen["A"] = 4
	s.pending["A"] = 1
	s.OnRequest(req(1, "A"))
	host.states["A"] = runtime.StateReady
	s.NoteProbe(ProbeNote{
		Token: s.recoverToken, Model: "A", Mode: RecoverCurrent,
		Class: runtime.RecReadyIdle, Ack: runtime.ApplyApplied, Applied: true,
		Active: 0, ProofGen: 3,
	})
	if _, ok := s.unresolved["A"]; !ok {
		t.Fatal("stale proof cleared unresolved")
	}
	if s.backend["A"] != 0 {
		t.Fatalf("backend=%v", s.backend)
	}
	host2 := &hostFake{fakeEffects: *newFake(map[string]runtime.State{"A": runtime.StateFailed})}
	host2.facts = RecoveryFacts{States: map[string]runtime.State{"A": runtime.StateFailed}}
	s2 := NewFIFO(testCfg(), &countPlan{}, host2)
	s2.unresolved["A"] = 1
	s2.dispatchGen["A"] = 4
	s2.quietGen["A"] = 4
	s2.pending["A"] = 1
	s2.OnRequest(req(2, "A"))
	host2.states["A"] = runtime.StateReady
	s2.NoteProbe(ProbeNote{
		Token: s2.recoverToken, Model: "A", Mode: RecoverCurrent,
		Class: runtime.RecReadyIdle, Ack: runtime.ApplyApplied, Applied: true,
		Active: 2, ProofGen: 4,
	})
	if s2.backend["A"] != 2 {
		t.Fatalf("backend=%d", s2.backend["A"])
	}
	if _, ok := s2.unresolved["A"]; !ok {
		t.Fatal("active requests cleared unresolved")
	}
	if len(host2.grants) != 0 {
		t.Fatalf("granted over the limit: %v", host2.grants)
	}
}

func TestSampleFailureAndQueueTimeoutReleasePhaseFirst(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{"A": runtime.StateStopped})}
	host.facts = RecoveryFacts{
		States:  map[string]runtime.State{"A": runtime.StateStopped},
		Markers: map[string]bool{"A": true},
	}
	plan := &countPlan{}
	s := NewFIFO(testCfg(), plan, host)
	s.OnRequest(req(2, "A"))
	if s.phase != phaseSample || plan.decides != 0 {
		t.Fatalf("phase=%d decides=%d", s.phase, plan.decides)
	}
	s.SampleFailed()
	if len(host.grants) != 1 || host.grants[0].err != ErrNotBooted {
		t.Fatalf("grants=%v", host.grants)
	}
	if plan.decides != 0 {
		t.Fatal("sample failure entered decide")
	}
}

func TestStoppedUnloadFailMarkerDoesNotRestatus(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateStopped,
		"B": runtime.StateStopped,
	})}
	host.facts = RecoveryFacts{
		States:  map[string]runtime.State{"A": runtime.StateStopped, "B": runtime.StateStopped},
		Markers: map[string]bool{"A": true},
	}
	s := NewFIFO(testCfg(), &countPlan{}, host)
	s.unloadFail["A"] = true
	s.OnRequest(req(1, "B"))
	if len(host.probes) != 0 || s.phase != phaseSample {
		t.Fatalf("probes=%v phase=%d", host.probes, s.phase)
	}
	if !s.unloadFail["A"] {
		t.Fatal("sticky cleared before the sample")
	}
}

func TestResourcesOutrankLifecycleAndActiveCodes(t *testing.T) {
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{
		"A": runtime.StateFailed,
		"B": runtime.StateFailed,
	})}
	host.facts = RecoveryFacts{States: map[string]runtime.State{
		"A": runtime.StateFailed,
		"B": runtime.StateFailed,
	}}
	s := NewFIFO(testCfg(), &countPlan{}, host)
	s.OnRequest(req(1, "A"))
	tok := s.recoverToken
	s.NoteProbe(ProbeNote{Token: tok, Model: "A", Mode: RecoverCurrent, Class: runtime.RecFailedNotResidentBusy, Ack: runtime.ApplyApplied, Applied: true})
	if host.probes[0] != "A" || len(host.probes) != 2 || host.probes[1] != "B" {
		t.Fatalf("probes=%v", host.probes)
	}
	s.NoteProbe(ProbeNote{Token: tok, Model: "B", Mode: RecoverCurrent, Class: runtime.RecReadyBusy, Ack: runtime.ApplyApplied, Applied: true})
	if len(host.grants) != 1 || host.grants[0].err != ErrResources {
		t.Fatalf("grants=%v", host.grants)
	}
}

func TestConcurrencyLimitAfterReadyRecovery(t *testing.T) {
	cfg := testCfg()
	m := cfg.Models["A"]
	m.ConcurrencyLimit = 1
	cfg.Models["A"] = m
	host := &hostFake{fakeEffects: *newFake(map[string]runtime.State{"A": runtime.StateFailed})}
	host.facts = RecoveryFacts{States: map[string]runtime.State{"A": runtime.StateFailed}}
	s := NewFIFO(cfg, &countPlan{}, host)
	s.OnRequest(req(1, "A"))
	s.OnRequest(req(2, "A"))
	host.states["A"] = runtime.StateReady
	s.NoteProbe(ProbeNote{Token: s.recoverToken, Model: "A", Mode: RecoverCurrent, Class: runtime.RecReadyIdle, Ack: runtime.ApplyApplied, Applied: true})
	if len(host.grants) != 1 {
		t.Fatalf("grants=%v", host.grants)
	}
	if s.pending["A"] != 1 {
		t.Fatalf("pending=%d", s.pending["A"])
	}
}
