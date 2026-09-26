package scheduler

import (
	"errors"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/runtime"
)

type phase int

const (
	phaseIdle phase = iota
	phaseUnloading
	phaseLoading
)

type FIFO struct {
	planner Swapper
	effects Effects
	limits  map[string]int
	maxQ    int

	nextID uint64
	queued []HandlerReq

	pending     map[string]int
	httpIn      map[string]int
	backend     map[string]int
	unresolved  map[string]int
	dispatchGen map[string]uint64
	quietGen    map[string]uint64
	gateClosed  map[string]bool
	gateAt      map[string]time.Time
	drain       time.Duration
	drainArmed  bool
	now         func() time.Time
	unloadFail  map[string]bool

	phase       phase
	swapTarget  string
	swapEvict   []string
	unloadBegan bool
}

func NewFIFO(cfg *config.Config, planner Swapper, effects Effects) *FIFO {
	limits := map[string]int{}
	maxQ := 256
	drain := 180 * time.Second
	if cfg != nil {
		if cfg.MaxQueueSize > 0 {
			maxQ = cfg.MaxQueueSize
		}
		if cfg.DrainTimeout > 0 {
			drain = cfg.DrainTimeout
		}
		for id, m := range cfg.Models {
			lim := m.ConcurrencyLimit
			if lim <= 0 {
				lim = 1
			}
			limits[id] = lim
		}
	}
	return &FIFO{
		planner:     planner,
		effects:     effects,
		limits:      limits,
		maxQ:        maxQ,
		pending:     map[string]int{},
		httpIn:      map[string]int{},
		backend:     map[string]int{},
		unresolved:  map[string]int{},
		dispatchGen: map[string]uint64{},
		quietGen:    map[string]uint64{},
		gateClosed:  map[string]bool{},
		gateAt:      map[string]time.Time{},
		drain:       drain,
		now:         time.Now,
		unloadFail:  map[string]bool{},
	}
}

func (s *FIFO) UseNow(now func() time.Time) {
	if now != nil {
		s.now = now
	}
}

func (s *FIFO) OnRequest(req HandlerReq) {
	if req.ID == 0 {
		s.nextID++
		req.ID = s.nextID
	}
	if _, ok := s.effects.ModelState(req.Model); !ok {
		s.rejectAdmit(req, ErrModelNotFound)
		return
	}
	if s.queuedLen() >= s.maxQ {
		s.rejectAdmit(req, ErrQueueFull)
		return
	}
	if !s.admit(req, nil) {
		return
	}
	s.queued = append(s.queued, req)
	s.tryDispatch()
}

func (s *FIFO) OnCancel(req HandlerReq) {
	for i, q := range s.queued {
		if q.ID == req.ID {
			s.queued = append(s.queued[:i], s.queued[i+1:]...)
			s.tryDispatch()
			return
		}
	}
}

func (s *FIFO) OnQueueTimeout(req HandlerReq) {
	for i, q := range s.queued {
		if q.ID == req.ID {
			s.queued = append(s.queued[:i], s.queued[i+1:]...)
			s.effects.GrantError(q, ErrQueueTimeout)
			s.tryDispatch()
			return
		}
	}
}

func (s *FIFO) OnProxyStart(ev ProxyStartEvent) {
	ok := s.pending[ev.ModelID] > 0
	if ok {
		s.pending[ev.ModelID]--
		if s.pending[ev.ModelID] == 0 {
			delete(s.pending, ev.ModelID)
		}
		s.httpIn[ev.ModelID]++
	}
	if ev.OK != nil {
		ev.OK <- ok
	}
}

func (s *FIFO) OnCancelGranted(ev CancelGrantedEvent) {
	if s.pending[ev.ModelID] > 0 {
		s.pending[ev.ModelID]--
		if s.pending[ev.ModelID] == 0 {
			delete(s.pending, ev.ModelID)
		}
	}
	s.tryDispatch()
}

func (s *FIFO) OnServeDone(ev ServeDoneEvent) {
	if s.httpIn[ev.ModelID] > 0 {
		s.httpIn[ev.ModelID]--
		if s.httpIn[ev.ModelID] == 0 {
			delete(s.httpIn, ev.ModelID)
		}
		s.unresolved[ev.ModelID]++
		if s.pending[ev.ModelID] == 0 && s.httpIn[ev.ModelID] == 0 {
			s.quietGen[ev.ModelID] = s.dispatchGen[ev.ModelID]
		}
	}
	s.tryDispatch()
}

func (s *FIFO) OnStatus(ev StatusEvent) {
	if ev.Err != nil {
		return
	}
	s.backend[ev.ModelID] = ev.Status.ActiveRequests
	if s.backend[ev.ModelID] <= 0 {
		delete(s.backend, ev.ModelID)
	}
	if ev.Status.ActiveRequests == 0 && s.pending[ev.ModelID] == 0 && s.httpIn[ev.ModelID] == 0 && ev.Gen != 0 && ev.Gen == s.dispatchGen[ev.ModelID] && ev.Gen == s.quietGen[ev.ModelID] {
		delete(s.unresolved, ev.ModelID)
	}
	s.tryDispatch()
}

func (s *FIFO) ProofGen(model string) uint64 {
	return s.quietGen[model]
}

func (s *FIFO) OnBackendDone(ev BackendDoneEvent) {
	delete(s.backend, ev.ModelID)
	s.tryDispatch()
}

func (s *FIFO) OnUnloadDone(ev UnloadDone) {
	if s.phase != phaseUnloading {
		return
	}
	if ev.Err != nil {
		for _, id := range s.swapEvict {
			if !errors.Is(ev.Err, ErrDrainTimeout) {
				s.unloadFail[id] = true
			}
		}
		s.phase = phaseIdle
		s.unloadBegan = false
		if len(s.queued) > 0 {
			head := s.queued[0]
			s.queued = s.queued[1:]
			if errors.Is(ev.Err, ErrDrainTimeout) {
				s.effects.GrantError(head, ErrDrainTimeout)
			} else {
				s.effects.GrantError(head, ErrUnloadBlocked)
			}
		}
		s.tryDispatch()
		return
	}
	for _, id := range s.swapEvict {
		s.openGate(id)
	}
	s.phase = phaseIdle
	s.unloadBegan = false
	s.swapTarget = ""
	s.swapEvict = nil
	s.reopenUnusedGates()
	s.tryDispatch()
}

func (s *FIFO) OnSwapDone(ev SwapDone) {
	if s.phase != phaseLoading || ev.ModelID != s.swapTarget {
		return
	}
	s.phase = phaseIdle
	s.unloadBegan = false
	target := s.swapTarget
	s.swapTarget = ""
	s.swapEvict = nil
	if ev.Err != nil {
		if len(s.queued) > 0 && s.queued[0].Model == target {
			head := s.queued[0]
			s.queued = s.queued[1:]
			grantErr := error(ErrLoadFailed)
			var se *Error
			if errors.As(ev.Err, &se) {
				grantErr = se
			}
			s.effects.GrantError(head, grantErr)
		}
		s.tryDispatch()
		return
	}
	s.tryDispatch()
}

func (s *FIFO) OnRemind() {
	s.drainArmed = false
	s.tryDispatch()
}

func (s *FIFO) OnShutdown(err error) {
	for _, q := range s.queued {
		s.effects.GrantError(q, err)
	}
	s.queued = nil
}

func (s *FIFO) tryDispatch() {
	s.reopenUnusedGates()
	if len(s.queued) == 0 {
		return
	}
	if s.phase != phaseIdle {
		return
	}
	head := s.queued[0]
	if s.blocked(head.Model) {
		s.queued = s.queued[1:]
		s.effects.GrantError(head, ErrUnloadBlocked)
		s.tryDispatch()
		return
	}
	if s.slots(head.Model) >= s.limit(head.Model) {
		return
	}

	running := s.runningIDs()
	evict := []string{}
	if d, ok := s.planner.(Decider); ok {
		var err error
		evict, err = d.Decide(head.Model, running)
		if err != nil {
			s.queued = s.queued[1:]
			s.effects.GrantError(head, err)
			s.tryDispatch()
			return
		}
	} else if s.planner != nil {
		evict = s.planner.EvictionFor(head.Model, running)
	}
	for _, v := range evict {
		s.closeGate(v)
	}
	if len(evict) > 0 {
		if s.drainExpired(evict) {
			s.failDrain(head, evict)
			return
		}
		if s.anyBusy(evict) {
			s.armDrain(evict)
			return
		}
		for _, v := range evict {
			if s.unloadFail[v] {
				s.queued = s.queued[1:]
				s.effects.GrantError(head, ErrUnloadBlocked)
				s.tryDispatch()
				return
			}
		}
		s.phase = phaseUnloading
		s.swapTarget = head.Model
		s.swapEvict = append([]string{}, evict...)
		s.unloadBegan = true
		s.drainArmed = false
		s.effects.StartUnload(evict, s.drainDeadline(evict))
		return
	}

	st, _ := s.effects.ModelState(head.Model)
	if st == runtime.StateReady {
		s.grantHead()
		s.tryDispatch()
		return
	}
	s.phase = phaseLoading
	s.swapTarget = head.Model
	s.effects.StartLoad(head.Model)
}

func (s *FIFO) grantHead() {
	head := s.queued[0]
	s.queued = s.queued[1:]
	if s.effects.GrantServe(head, head.Model) {
		s.dispatchGen[head.Model]++
		delete(s.quietGen, head.Model)
		s.pending[head.Model]++
		return
	}
}

func (s *FIFO) reopenUnusedGates() {
	need := map[string]bool{}
	if len(s.queued) > 0 && s.phase == phaseIdle {
		head := s.queued[0]
		running := s.runningIDs()
		if s.planner != nil {
			for _, v := range s.planner.EvictionFor(head.Model, running) {
				need[v] = true
			}
		}
	}
	if s.phase != phaseIdle && s.unloadBegan {
		for _, v := range s.swapEvict {
			need[v] = true
		}
	}
	for id := range s.gateClosed {
		if !need[id] && !(s.unloadBegan && s.phase != phaseIdle) {
			s.openGate(id)
		}
	}
}

func (s *FIFO) closeGate(id string) {
	if !s.gateClosed[id] {
		s.gateAt[id] = s.now()
	}
	s.gateClosed[id] = true
}

func (s *FIFO) openGate(id string) {
	delete(s.gateClosed, id)
	delete(s.gateAt, id)
}

func (s *FIFO) drainDeadline(ids []string) time.Time {
	deadline := s.now().Add(s.drain)
	for _, id := range ids {
		at, ok := s.gateAt[id]
		if !ok {
			continue
		}
		end := at.Add(s.drain)
		if end.Before(deadline) {
			deadline = end
		}
	}
	return deadline
}

func (s *FIFO) drainExpired(ids []string) bool {
	if s.drain <= 0 {
		return false
	}
	now := s.now()
	for _, id := range ids {
		at, ok := s.gateAt[id]
		if ok && !now.Before(at.Add(s.drain)) {
			return true
		}
	}
	return false
}

func (s *FIFO) armDrain(ids []string) {
	if s.drainArmed {
		return
	}
	s.drainArmed = true
	s.effects.Remind(s.drainDeadline(ids))
}

func (s *FIFO) failDrain(head HandlerReq, evict []string) {
	for _, id := range evict {
		s.openGate(id)
	}
	s.drainArmed = false
	s.queued = s.queued[1:]
	s.effects.GrantError(head, ErrDrainTimeout)
	s.tryDispatch()
}

func (s *FIFO) runningIDs() []string {
	var ids []string
	if s.swapTarget != "" && (s.phase == phaseLoading || s.phase == phaseUnloading) {
		seen := map[string]bool{s.swapTarget: true}
		ids = append(ids, s.swapTarget)
		for id, st := range s.effects.RunningModels() {
			if seen[id] {
				continue
			}
			if st != runtime.StateStopped && st != runtime.StateUnknown && st != runtime.StateShutdown {
				ids = append(ids, id)
				seen[id] = true
			}
		}
		return ids
	}
	for id, st := range s.effects.RunningModels() {
		if st != runtime.StateStopped && st != runtime.StateUnknown && st != runtime.StateShutdown {
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *FIFO) anyBusy(ids []string) bool {
	for _, id := range ids {
		if s.pending[id]+s.httpIn[id]+s.unresolved[id]+s.backend[id] > 0 {
			return true
		}
	}
	return false
}

func (s *FIFO) Protected(id string) bool {
	return s.pending[id]+s.httpIn[id]+s.unresolved[id]+s.backend[id] > 0
}

func (s *FIFO) slots(model string) int {
	return s.pending[model] + s.httpIn[model] + s.unresolved[model]
}

func (s *FIFO) limit(model string) int {
	if n, ok := s.limits[model]; ok && n > 0 {
		return n
	}
	return 1
}

func (s *FIFO) blocked(model string) bool {
	running := s.runningIDs()
	if s.planner == nil {
		return s.unloadFail[model]
	}
	for _, v := range s.planner.EvictionFor(model, running) {
		if s.unloadFail[v] {
			return true
		}
	}
	return false
}

func (s *FIFO) queuedLen() int { return len(s.queued) }

func (s *FIFO) QueueDepth() int { return len(s.queued) }

func (s *FIFO) admit(req HandlerReq, err error) bool {
	if req.Admit == nil {
		return err == nil
	}
	select {
	case <-reqDone(req):
		return false
	default:
	}
	select {
	case req.Admit <- err:
		return err == nil
	case <-reqDone(req):
		return false
	}
}

func (s *FIFO) rejectAdmit(req HandlerReq, err error) {
	if req.Admit != nil {
		select {
		case req.Admit <- err:
		default:
			s.effects.GrantError(req, err)
		}
		return
	}
	s.effects.GrantError(req, err)
}

func reqDone(req HandlerReq) <-chan struct{} {
	if req.Ctx == nil {
		return nil
	}
	return req.Ctx.Done()
}

func (s *FIFO) QueueLen() int             { return len(s.queued) }
func (s *FIFO) Pending(id string) int     { return s.pending[id] }
func (s *FIFO) HTTPIn(id string) int      { return s.httpIn[id] }
func (s *FIFO) GateClosed(id string) bool { return s.gateClosed[id] }
func (s *FIFO) PhaseUnloading() bool      { return s.phase == phaseUnloading }
func (s *FIFO) PhaseLoading() bool        { return s.phase == phaseLoading }
