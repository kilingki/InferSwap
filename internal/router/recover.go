package router

import (
	"context"
	"time"

	"github.com/kilingki/InferSwap/internal/router/scheduler"
	"github.com/kilingki/InferSwap/internal/runtime"
)

type recoverEvent struct {
	Token       uint64
	Model       string
	Gen         uint64
	CompletedAt time.Time
	Status      runtime.Status
	Err         error
	Kind        runtime.StatusErrKind
	Class       runtime.RecoveryClass
	ProofGen    uint64
}

type ledgerSnap struct {
	reserved   int64
	reservedOK bool
	marker     time.Time
	markerOK   bool
}

func (r *Router) RecoveryFacts() scheduler.RecoveryFacts {
	facts := scheduler.RecoveryFacts{
		States:   map[string]runtime.State{},
		Markers:  map[string]bool{},
		InFlight: map[string]bool{},
	}
	for id, rt := range r.runtimes {
		facts.States[id] = rt.State()
		if _, ok := r.residualAfter[id]; ok {
			facts.Markers[id] = true
		}
		if c, ok := rt.(*runtime.Client); ok && c.LoadInFlight() {
			facts.InFlight[id] = true
		}
	}
	return facts
}

func (r *Router) BeginProbe(token uint64, model string) {
	rt := r.runtimes[model]
	c, ok := rt.(*runtime.Client)
	if !ok {
		return
	}
	gen := c.Generation()
	proof := r.schedule.ProofGen(model)
	timeout := r.statusTimeout()
	go func() {
		ctx, cancel := context.WithTimeout(r.shutdownCtx, timeout)
		defer cancel()
		st, err := c.Status(ctx)
		ev := recoverEvent{
			Token:       token,
			Model:       model,
			Gen:         gen,
			CompletedAt: r.clock.Now(),
			Status:      st,
			Err:         err,
			Kind:        runtime.ClassifyStatusErr(err),
			ProofGen:    proof,
		}
		if err == nil {
			ev.Class = runtime.ClassifyRecovery(st)
		}
		select {
		case r.recoverCh <- ev:
		case <-r.shutdownCtx.Done():
		}
	}()
}

func (r *Router) onRecover(ev recoverEvent) {
	mode := r.schedule.RecoverMode(ev.Token)
	if mode == scheduler.RecoverDrop {
		r.schedule.ForgetProbe(ev.Model)
		return
	}
	snap := r.snapLedger(ev.Model)
	if ev.Kind != runtime.StatusErrEndpointDown {
		r.tentative(ev)
	}
	ack := runtime.ApplyApplied
	var shutdownDone chan struct{}
	callClient := ev.Kind == runtime.StatusErrNone && ev.Class != runtime.RecInvalid
	if callClient {
		ack, shutdownDone = r.applyClient(ev)
		if shutdownDone != nil && ack == runtime.ApplyCode(-1) {
			r.restoreLedger(ev.Model, snap)
			r.finishShutdown(shutdownDone)
			return
		}
	}
	applied := callClient && ack == runtime.ApplyApplied
	if shutdownDone != nil {
		if applied {
			r.confirmRecover(ev)
		} else if callClient {
			r.restoreLedger(ev.Model, snap)
		}
		r.finishShutdown(shutdownDone)
		return
	}
	if callClient && !applied {
		r.restoreLedger(ev.Model, snap)
		if ack == runtime.ApplyBusy {
			r.ensurePeak(ev.Model)
		}
	} else if applied {
		r.confirmRecover(ev)
	}
	r.schedule.NoteProbe(scheduler.ProbeNote{
		Token:    ev.Token,
		Model:    ev.Model,
		Mode:     mode,
		Class:    ev.Class,
		Kind:     ev.Kind,
		Ack:      ack,
		Active:   ev.Status.ActiveRequests,
		ProofGen: ev.ProofGen,
		Applied:  applied,
	})
}

func (r *Router) applyClient(ev recoverEvent) (runtime.ApplyCode, chan struct{}) {
	c, ok := r.runtimes[ev.Model].(*runtime.Client)
	if !ok {
		return runtime.ApplyStale, nil
	}
	ackCh := make(chan runtime.ApplyCode, 1)
	req := runtime.ApplyReq{
		Cmd: runtime.ApplyCmd{
			Gen:       ev.Gen,
			Class:     ev.Class,
			Status:    ev.Status,
			HasStatus: true,
		},
		Ack: ackCh,
	}
	select {
	case c.RecoveryReqs() <- req:
	case done := <-r.shutdownCh:
		return runtime.ApplyCode(-1), done
	}
	return waitApplyAck(ackCh, r.shutdownCh)
}

func waitApplyAck(ackCh <-chan runtime.ApplyCode, shutdown <-chan chan struct{}) (runtime.ApplyCode, chan struct{}) {
	select {
	case ack := <-ackCh:
		select {
		case done := <-shutdown:
			return ack, done
		default:
			return ack, nil
		}
	case done := <-shutdown:
		ack := <-ackCh
		return ack, done
	}
}

func (r *Router) snapLedger(id string) ledgerSnap {
	snap := ledgerSnap{}
	if v, ok := r.reserved[id]; ok {
		snap.reserved = v
		snap.reservedOK = true
	}
	if v, ok := r.residualAfter[id]; ok {
		snap.marker = v
		snap.markerOK = true
	}
	return snap
}

func (r *Router) restoreLedger(id string, snap ledgerSnap) {
	if snap.reservedOK {
		r.reserved[id] = snap.reserved
	} else {
		delete(r.reserved, id)
	}
	if snap.markerOK {
		r.residualAfter[id] = snap.marker
	} else {
		delete(r.residualAfter, id)
	}
}

func (r *Router) ensurePeak(id string) {
	m, ok := r.cfg.Models[id]
	if !ok {
		return
	}
	peak := m.LoadBound()
	if cur, ok := r.reserved[id]; ok && cur >= peak {
		return
	}
	r.reserved[id] = peak
}

func (r *Router) tentative(ev recoverEvent) {
	switch ev.Class {
	case runtime.RecReadyIdle:
		delete(r.residualAfter, ev.Model)
		r.ensurePeak(ev.Model)
	case runtime.RecUnloadedIdle, runtime.RecFailedNotResidentIdle:
		r.ensurePeak(ev.Model)
		if prev, ok := r.residualAfter[ev.Model]; !ok || ev.CompletedAt.After(prev) {
			r.residualAfter[ev.Model] = ev.CompletedAt
		}
	default:
		r.ensurePeak(ev.Model)
	}
}

func (r *Router) confirmRecover(ev recoverEvent) {
	cp := ev.Status
	if cp.LastError != nil {
		le := *cp.LastError
		cp.LastError = &le
	}
	r.lastStatus[ev.Model] = cp
	if ev.Class == runtime.RecReadyIdle {
		r.noteRecoveredReady(ev.Model)
	}
}

func (r *Router) finishShutdown(done chan struct{}) {
	r.schedule.OnShutdown(scheduler.ErrShutdown)
	if done != nil {
		close(done)
	}
}
