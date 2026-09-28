package scheduler

import (
	"sort"

	"github.com/kilingki/InferSwap/internal/runtime"
)

// RecoveryHost is implemented by the router. FIFO tests that do not implement
// it keep the pre-recovery dispatch path.
type RecoveryHost interface {
	RecoveryFacts() RecoveryFacts
	BeginProbe(token uint64, model string)
}

type RecoveryFacts struct {
	States   map[string]runtime.State
	Markers  map[string]bool
	InFlight map[string]bool
}

type RecoverMode int

const (
	RecoverDrop RecoverMode = iota
	RecoverCurrent
	RecoverLate
)

type ProbeNote struct {
	Token    uint64
	Model    string
	Mode     RecoverMode
	Class    runtime.RecoveryClass
	Kind     runtime.StatusErrKind
	Ack      runtime.ApplyCode
	Active   int
	ProofGen uint64
	Applied  bool
}

func (s *FIFO) setPhase(p phase) {
	if p != phaseIdle {
		s.acceptLate = map[uint64]bool{}
	}
	s.phase = p
}

func (s *FIFO) dropOwner(id uint64) {
	if s.recoverOwner != id {
		return
	}
	if s.phase != phaseRecover && s.phase != phaseSample {
		return
	}
	if s.recoverToken != 0 {
		if s.acceptLate == nil {
			s.acceptLate = map[uint64]bool{}
		}
		s.acceptLate[s.recoverToken] = true
	}
	s.phase = phaseIdle
	s.recoverOwner = 0
	s.recoverToken = 0
}

func (s *FIFO) maybeRecover(head HandlerReq) bool {
	host, ok := s.effects.(RecoveryHost)
	if !ok {
		return false
	}
	facts := host.RecoveryFacts()
	st := facts.States[head.Model]
	if st == runtime.StateReady {
		return false
	}
	if st == runtime.StateStarting && facts.InFlight[head.Model] {
		return false
	}
	status, samples := planRecovery(head.Model, facts, s.unloadFail)
	status = filterSeen(s.seen[head.ID], status)
	if len(status) == 0 && len(samples) == 0 {
		return false
	}
	for _, id := range status {
		if tok, live := s.probeStarted[id]; live {
			s.setPhase(phaseRecover)
			s.recoverToken = tok
			s.recoverOwner = head.ID
			s.recoverModel = head.Model
			s.statusOrder = status
			s.statusPos = 0
			s.needSample = setOf(samples)
			s.checked = map[string]bool{}
			s.sawResources = false
			s.sawLifecycle = false
			s.skipRest = false
			s.inflightModel = id
			return true
		}
	}
	s.tokenSeq++
	tok := s.tokenSeq
	s.setPhase(phaseRecover)
	s.recoverToken = tok
	s.recoverOwner = head.ID
	s.recoverModel = head.Model
	s.statusOrder = status
	s.statusPos = 0
	s.needSample = setOf(samples)
	s.checked = map[string]bool{}
	s.sawResources = false
	s.sawLifecycle = false
	s.skipRest = false
	s.inflightModel = ""
	if len(status) == 0 {
		s.setPhase(phaseSample)
		return true
	}
	s.launchProbe(host)
	return true
}

func (s *FIFO) markSeen(id uint64, model string) {
	if id == 0 || model == "" {
		return
	}
	if s.seen == nil {
		s.seen = map[uint64]map[string]bool{}
	}
	if s.seen[id] == nil {
		s.seen[id] = map[string]bool{}
	}
	s.seen[id][model] = true
}

func filterSeen(seen map[string]bool, ids []string) []string {
	if len(seen) == 0 {
		return ids
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			out = append(out, id)
		}
	}
	return out
}

func setOf(ids []string) map[string]bool {
	out := map[string]bool{}
	for _, id := range ids {
		out[id] = true
	}
	return out
}

func planRecovery(target string, facts RecoveryFacts, unloadFail map[string]bool) (status []string, samples []string) {
	ids := make([]string, 0, len(facts.States))
	for id := range facts.States {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	want := map[string]bool{}
	for _, id := range ids {
		st := facts.States[id]
		if facts.InFlight[id] && st == runtime.StateStarting {
			continue
		}
		if st == runtime.StateFailed || st == runtime.StateUnknown || unloadFail[id] {
			want[id] = true
		}
	}
	for _, id := range ids {
		if facts.Markers[id] {
			samples = append(samples, id)
			delete(want, id)
		}
	}
	if want[target] {
		status = append(status, target)
		delete(want, target)
	}
	rest := make([]string, 0, len(want))
	for id := range want {
		rest = append(rest, id)
	}
	sort.Strings(rest)
	status = append(status, rest...)
	return status, samples
}

func (s *FIFO) launchProbe(host RecoveryHost) {
	for s.statusPos < len(s.statusOrder) {
		id := s.statusOrder[s.statusPos]
		if s.checked[id] {
			s.statusPos++
			continue
		}
		s.inflightModel = id
		s.probeStarted[id] = s.recoverToken
		s.markSeen(s.recoverOwner, id)
		host.BeginProbe(s.recoverToken, id)
		return
	}
	s.finishProbes()
}

func (s *FIFO) RecoverMode(token uint64) RecoverMode {
	if s.phase == phaseRecover && token == s.recoverToken {
		return RecoverCurrent
	}
	if s.phase == phaseIdle && s.acceptLate[token] {
		return RecoverLate
	}
	return RecoverDrop
}

func (s *FIFO) ForgetProbe(model string) {
	delete(s.probeStarted, model)
}

func (s *FIFO) NoteProbe(n ProbeNote) {
	delete(s.probeStarted, n.Model)
	if n.Mode == RecoverLate {
		s.noteLate(n)
		return
	}
	s.markSeen(s.recoverOwner, n.Model)
	if s.phase != phaseRecover || n.Token != s.recoverToken {
		return
	}
	s.checked[n.Model] = true
	s.inflightModel = ""
	if n.Ack == runtime.ApplyStale {
		s.failOwner(ErrResources)
		return
	}
	if n.Ack == runtime.ApplyBusy {
		if n.Model == s.recoverModel {
			st, _ := s.effects.ModelState(n.Model)
			if st == runtime.StateStarting {
				s.phase = phaseIdle
				s.recoverToken = 0
				s.recoverOwner = 0
				s.tryDispatch()
				return
			}
		}
		s.failOwner(ErrLifecyclePending)
		return
	}
	s.absorb(n)
	if s.skipRest {
		s.phase = phaseIdle
		s.recoverToken = 0
		s.recoverOwner = 0
		s.tryDispatch()
		return
	}
	s.statusPos++
	host, _ := s.effects.(RecoveryHost)
	s.launchProbe(host)
}

func (s *FIFO) absorb(n ProbeNote) {
	if n.Kind == runtime.StatusErrEndpointDown {
		if n.Model != s.recoverModel {
			s.sawResources = true
		}
		return
	}
	if n.Kind == runtime.StatusErrOther || n.Class == runtime.RecInvalid {
		s.sawResources = true
		return
	}
	if n.Applied {
		s.applyBackend(n.Model, n.Active, n.ProofGen)
	}
	switch n.Class {
	case runtime.RecReadyIdle:
		if n.Applied {
			delete(s.unloadFail, n.Model)
			delete(s.needSample, n.Model)
		}
		if n.Model == s.recoverModel {
			s.skipRest = true
		}
	case runtime.RecUnloadedIdle, runtime.RecFailedNotResidentIdle:
		if n.Applied {
			s.needSample[n.Model] = true
		}
	case runtime.RecReadyBusy, runtime.RecInProgress:
		s.sawLifecycle = true
	case runtime.RecFailedOccupied, runtime.RecFailedNotResidentBusy:
		s.sawResources = true
	default:
		s.sawResources = true
	}
}

func (s *FIFO) applyBackend(model string, active int, proof uint64) {
	if active <= 0 {
		delete(s.backend, model)
	} else {
		s.backend[model] = active
	}
	if active == 0 && s.pending[model] == 0 && s.httpIn[model] == 0 && proof != 0 && proof == s.dispatchGen[model] && proof == s.quietGen[model] {
		delete(s.unresolved, model)
	}
}

func (s *FIFO) noteLate(n ProbeNote) {
	if n.Ack == runtime.ApplyStale {
		return
	}
	if n.Applied {
		s.applyBackend(n.Model, n.Active, n.ProofGen)
		if n.Class == runtime.RecReadyIdle {
			delete(s.unloadFail, n.Model)
		}
	}
	if s.phase == phaseIdle {
		s.tryDispatch()
	}
}

func (s *FIFO) finishProbes() {
	if s.skipRest {
		s.phase = phaseIdle
		s.recoverToken = 0
		s.recoverOwner = 0
		s.tryDispatch()
		return
	}
	if s.sawResources {
		s.failOwner(ErrResources)
		return
	}
	if s.sawLifecycle {
		s.failOwner(ErrLifecyclePending)
		return
	}
	if len(s.needSample) > 0 {
		s.setPhase(phaseSample)
		return
	}
	s.phase = phaseIdle
	s.recoverToken = 0
	s.recoverOwner = 0
	s.tryDispatch()
}

func (s *FIFO) failOwner(err error) {
	owner := s.recoverOwner
	s.phase = phaseIdle
	s.recoverToken = 0
	s.recoverOwner = 0
	s.inflightModel = ""
	if len(s.queued) > 0 && s.queued[0].ID == owner {
		head := s.queued[0]
		s.queued = s.queued[1:]
		s.effects.GrantError(head, err)
	}
	s.tryDispatch()
}

func (s *FIFO) MarkerCleared(id string) {
	delete(s.needSample, id)
	delete(s.unloadFail, id)
	if s.phase == phaseSample && len(s.needSample) == 0 {
		s.phase = phaseIdle
		s.recoverToken = 0
		s.recoverOwner = 0
		s.tryDispatch()
	}
}

func (s *FIFO) SampleFailed() {
	if s.phase != phaseSample {
		return
	}
	s.failOwner(ErrNotBooted)
}
