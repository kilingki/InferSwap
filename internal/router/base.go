package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/resource"
	"github.com/kilingki/InferSwap/internal/router/scheduler"
	"github.com/kilingki/InferSwap/internal/runtime"
	"github.com/kilingki/InferSwap/internal/testkit"
)

type Router struct {
	cfg      *config.Config
	runtimes map[string]runtime.Runtime
	schedule *scheduler.FIFO
	clock    testkit.Clock

	shutdownCtx context.Context
	shutdownFn  context.CancelFunc
	shutting    atomic.Bool

	handlerCh       chan scheduler.HandlerReq
	cancelCh        chan scheduler.HandlerReq
	swapDoneCh      chan scheduler.SwapDone
	unloadDoneCh    chan scheduler.UnloadDone
	serveDoneCh     chan scheduler.ServeDoneEvent
	proxyStartCh    chan scheduler.ProxyStartEvent
	cancelGrantedCh chan scheduler.CancelGrantedEvent
	statusCh        chan scheduler.StatusEvent
	backendDoneCh   chan scheduler.BackendDoneEvent
	timeoutCh       chan scheduler.HandlerReq
	shutdownCh      chan chan struct{}
	runDone         chan struct{}
	lastStatus      map[string]runtime.Status
	nextReq         atomic.Uint64

	observer       resource.Observer
	device         string
	snap           resource.Snapshot
	observeCh      chan resource.Snapshot
	reserved       map[string]int64
	lastUse        map[string]time.Time
	loadedAt       map[string]time.Time
	loadSerial     string
	bootGPU        bool
	admitCh        chan admitReq
	unconfirmed    []string
	residualAfter  map[string]time.Time
	unloadNoticeCh chan unloadNotice
	syncCh         chan func()
}

type admitReq struct {
	model string
	resp  chan error
}

type unloadNotice struct {
	ev     scheduler.UnloadDone
	sample resource.Snapshot
	at     time.Time
}

func NewExclusive(cfg *config.Config, runtimes map[string]runtime.Runtime, clock testkit.Clock) *Router {
	return New(cfg, runtimes, clock, nil)
}

func New(cfg *config.Config, runtimes map[string]runtime.Runtime, clock testkit.Clock, obs resource.Observer) *Router {
	if clock == nil {
		clock = testkit.RealClock{}
	}
	device := "fake"
	if cfg != nil && cfg.GPU.Device != "" {
		device = cfg.GPU.Device
	}
	if obs == nil {
		if cfg != nil && cfg.GPU.Device != "" {
			obs = resource.NVML{Device: cfg.GPU.Device}
		} else {
			obs = resource.Generous(device)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{
		cfg:             cfg,
		runtimes:        runtimes,
		clock:           clock,
		shutdownCtx:     ctx,
		shutdownFn:      cancel,
		handlerCh:       make(chan scheduler.HandlerReq),
		cancelCh:        make(chan scheduler.HandlerReq),
		swapDoneCh:      make(chan scheduler.SwapDone),
		unloadDoneCh:    make(chan scheduler.UnloadDone),
		serveDoneCh:     make(chan scheduler.ServeDoneEvent),
		proxyStartCh:    make(chan scheduler.ProxyStartEvent),
		cancelGrantedCh: make(chan scheduler.CancelGrantedEvent),
		statusCh:        make(chan scheduler.StatusEvent),
		backendDoneCh:   make(chan scheduler.BackendDoneEvent),
		timeoutCh:       make(chan scheduler.HandlerReq),
		shutdownCh:      make(chan chan struct{}),
		runDone:         make(chan struct{}),
		lastStatus:      map[string]runtime.Status{},
		observer:        obs,
		reserved:        map[string]int64{},
		lastUse:         map[string]time.Time{},
		loadedAt:        map[string]time.Time{},
		admitCh:         make(chan admitReq),
		device:          device,
		observeCh:       make(chan resource.Snapshot, 1),
		residualAfter:   map[string]time.Time{},
		unloadNoticeCh:  make(chan unloadNotice),
		syncCh:          make(chan func()),
	}
	r.schedule = scheduler.NewFIFO(cfg, r, r)
	r.schedule.UseNow(r.clock.Now)
	for id, rt := range runtimes {
		if c, ok := rt.(*runtime.Client); ok {
			modelID := id
			c.SetLoadGate(func(ctx context.Context) error { return r.WaitLoad(ctx, modelID) })
		}
	}
	if snap, err := obs.Observe(); err == nil && snap.DeviceID == device && resource.Fresh(snap, time.Now(), r.observeAge()) {
		r.snap = snap
		r.bootGPU = true
	}
	go r.pollGPU()
	go r.run()
	return r
}

func (r *Router) pollGPU() {
	for {
		snap, err := r.observer.Observe()
		if err != nil || !snap.OK || snap.DeviceID != r.device {
			snap = resource.Snapshot{}
		}
		select {
		case r.observeCh <- snap:
		case <-r.shutdownCtx.Done():
			return
		}
		select {
		case <-time.After(time.Second):
		case <-r.shutdownCtx.Done():
			return
		}
	}
}

func (r *Router) run() {
	defer close(r.runDone)
	for {
		select {
		case <-r.shutdownCtx.Done():
			return
		case done := <-r.shutdownCh:
			r.schedule.OnShutdown(scheduler.ErrShutdown)
			if done != nil {
				close(done)
			}
		case req := <-r.handlerCh:
			r.schedule.OnRequest(req)
		case req := <-r.cancelCh:
			r.schedule.OnCancel(req)
		case ev := <-r.unloadDoneCh:
			r.finishUnload(unloadNotice{ev: ev})
		case notice := <-r.unloadNoticeCh:
			r.finishUnload(notice)
		case ev := <-r.swapDoneCh:
			if ev.Err != nil {
				slog.Info("load failed", "model", ev.ModelID, "err", ev.Err)
			} else {
				slog.Info("load ready", "model", ev.ModelID)
				r.noteLoaded(ev.ModelID)
			}
			if r.loadSerial == ev.ModelID {
				r.loadSerial = ""
			}
			r.schedule.OnSwapDone(ev)
		case ev := <-r.serveDoneCh:
			r.noteUse(ev.ModelID)
			r.schedule.OnServeDone(ev)
			go r.refreshStatus(ev.ModelID, r.schedule.ProofGen(ev.ModelID))
		case ev := <-r.proxyStartCh:
			r.schedule.OnProxyStart(ev)
		case ev := <-r.cancelGrantedCh:
			r.schedule.OnCancelGranted(ev)
		case ev := <-r.statusCh:
			if ev.Err == nil {
				r.lastStatus[ev.ModelID] = ev.Status
			}
			r.schedule.OnStatus(ev)
		case ev := <-r.backendDoneCh:
			r.schedule.OnBackendDone(ev)
		case req := <-r.timeoutCh:
			r.schedule.OnQueueTimeout(req)
		case req := <-r.admitCh:
			req.resp <- r.grantLoad(req.model)
		case snap := <-r.observeCh:
			r.applyObservation(snap)
		case fn := <-r.syncCh:
			fn()
		}
	}
}

func (r *Router) onLoop(fn func()) {
	done := make(chan struct{})
	select {
	case r.syncCh <- func() {
		defer close(done)
		fn()
	}:
	case <-r.shutdownCtx.Done():
		return
	}
	select {
	case <-done:
	case <-r.shutdownCtx.Done():
	}
}

func (r *Router) applyObservation(snap resource.Snapshot) {
	if !snap.OK || snap.DeviceID != r.device {
		r.snap = resource.Snapshot{}
		r.bootGPU = false
		return
	}
	r.snap = snap
	r.bootGPU = true
	for id, at := range r.residualAfter {
		if snap.ObservedAt.After(at) {
			r.holdResidual(id)
			delete(r.residualAfter, id)
		}
	}
}

func (r *Router) finishUnload(notice unloadNotice) {
	ev := notice.ev
	if ev.Err == nil {
		slog.Info("unload confirmed", "models", ev.IDs)
		for _, id := range ev.IDs {
			r.residualAfter[id] = notice.at
		}
		r.applyObservation(notice.sample)
	} else {
		slog.Info("unload failed", "models", ev.IDs, "err", ev.Err)
	}
	r.schedule.OnUnloadDone(ev)
}

func (r *Router) refreshStatus(id string, gen uint64) {
	rt, ok := r.runtimes[id]
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.statusTimeout())
	defer cancel()
	st, err := rt.Status(ctx)
	select {
	case r.statusCh <- scheduler.StatusEvent{ModelID: id, Status: st, Err: err, Gen: gen}:
	case <-r.shutdownCtx.Done():
	}
}

func (r *Router) statusTimeout() time.Duration {
	if r.cfg != nil && r.cfg.StatusTimeout > 0 {
		return r.cfg.StatusTimeout
	}
	return 5 * time.Second
}

func (r *Router) ModelState(modelID string) (runtime.State, bool) {
	rt, ok := r.runtimes[modelID]
	if !ok {
		return "", false
	}
	return rt.State(), true
}

func (r *Router) RunningModels() map[string]runtime.State {
	out := map[string]runtime.State{}
	for id, rt := range r.runtimes {
		st := rt.State()
		if st != runtime.StateStopped && st != runtime.StateUnknown && st != runtime.StateShutdown {
			out[id] = st
		}
	}
	return out
}

func (r *Router) LastStatus(modelID string) (runtime.Status, bool) {
	st, ok := r.lastStatus[modelID]
	return st, ok
}

func (r *Router) Remind(at time.Time) {
	d := at.Sub(r.clock.Now())
	go func() {
		select {
		case <-r.clock.After(d):
		case <-r.shutdownCtx.Done():
			return
		}
		select {
		case r.syncCh <- func() { r.schedule.OnRemind() }:
		case <-r.shutdownCtx.Done():
		}
	}()
}

func (r *Router) StartUnload(ids []string, deadline time.Time) {
	if deadline.IsZero() {
		deadline = r.clock.Now().Add(r.drainBudget())
	}
	go func() {
		var first error
		var at time.Time
		for _, id := range ids {
			rt, ok := r.runtimes[id]
			if !ok {
				continue
			}
			if err := r.waitIdle(rt, deadline); err != nil {
				if first == nil {
					first = err
				}
				break
			}
			to := time.Second
			if m, ok := r.cfg.Models[id]; ok && m.UnloadTimeout > 0 {
				to = m.UnloadTimeout
			} else if r.cfg != nil && r.cfg.UnloadTimeout > 0 {
				to = r.cfg.UnloadTimeout
			}
			if err := rt.Stop(context.Background(), to); err != nil && first == nil {
				first = err
				break
			}
			at = r.clock.Now()
		}
		if first != nil || at.IsZero() {
			if first == nil {
				first = fmt.Errorf("unload did not complete")
			}
			r.sendUnload(unloadNotice{ev: scheduler.UnloadDone{IDs: ids, Err: first}})
			return
		}
		snap, serr := r.sampleAfter(at)
		notice := unloadNotice{ev: scheduler.UnloadDone{IDs: ids}, at: at, sample: snap}
		if serr != nil {
			notice.ev.Err = serr
		}
		r.sendUnload(notice)
	}()
}

func (r *Router) sendUnload(notice unloadNotice) {
	select {
	case r.unloadNoticeCh <- notice:
	case <-r.shutdownCtx.Done():
	}
}

func (r *Router) sampleAfter(after time.Time) (resource.Snapshot, error) {
	deadline := r.clock.Now().Add(r.drainBudget())
	for {
		snap, err := r.observer.Observe()
		if err == nil && snap.OK && snap.DeviceID == r.device && snap.ObservedAt.After(after) {
			return snap, nil
		}
		if !r.clock.Now().Before(deadline) {
			return resource.Snapshot{}, fmt.Errorf("gpu sample after unload not observed")
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-r.shutdownCtx.Done():
			return resource.Snapshot{}, scheduler.ErrShutdown
		}
	}
}

func (r *Router) waitIdle(rt runtime.Runtime, deadline time.Time) error {
	for {
		if !time.Now().Before(deadline) {
			return scheduler.ErrDrainTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), r.statusTimeout())
		st, err := rt.Status(ctx)
		cancel()
		if err == nil && st.ActiveRequests == 0 && st.State != runtime.WireLoading && st.State != runtime.WireUnloading {
			return nil
		}
		if err != nil || !time.Now().Before(deadline) {
			return scheduler.ErrDrainTimeout
		}
		wait := time.Second
		if until := time.Until(deadline); until < wait {
			wait = until
		}
		select {
		case <-time.After(wait):
		case <-r.shutdownCtx.Done():
			return scheduler.ErrShutdown
		}
	}
}

func (r *Router) drainBudget() time.Duration {
	if r.cfg != nil && r.cfg.DrainTimeout > 0 {
		return r.cfg.DrainTimeout
	}
	return 180 * time.Second
}

func (r *Router) StartLoad(modelID string) {
	go func() {
		rt := r.runtimes[modelID]
		to := time.Second
		if m, ok := r.cfg.Models[modelID]; ok && m.HealthCheckTimeout > 0 {
			to = m.HealthCheckTimeout
		}
		err := rt.EnsureReady(r.shutdownCtx, to)
		select {
		case r.swapDoneCh <- scheduler.SwapDone{ModelID: modelID, Err: err}:
		case <-r.shutdownCtx.Done():
		}
	}()
}

func (r *Router) GrantError(req scheduler.HandlerReq, err error) {
	slog.Info("admission denied", "model", req.Model, "err", err)
	r.grant(req, scheduler.HandlerResp{Err: err})
}

func (r *Router) GrantServe(req scheduler.HandlerReq, modelID string) bool {
	rt := r.runtimes[modelID]
	return r.grant(req, scheduler.HandlerResp{HandleFunc: r.trackedServe(modelID, rt)})
}

func (r *Router) grant(req scheduler.HandlerReq, resp scheduler.HandlerResp) bool {
	select {
	case req.Respond <- resp:
		return true
	case <-req.Ctx.Done():
		return false
	case <-r.shutdownCtx.Done():
		return false
	}
}

func (r *Router) trackedServe(modelID string, rt runtime.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ok := make(chan bool, 1)
		select {
		case r.proxyStartCh <- scheduler.ProxyStartEvent{ModelID: modelID, OK: ok}:
		case <-req.Context().Done():
			select {
			case r.cancelGrantedCh <- scheduler.CancelGrantedEvent{ModelID: modelID}:
			case <-r.shutdownCtx.Done():
			}
			return
		case <-r.shutdownCtx.Done():
			return
		}
		if !<-ok {
			return
		}
		defer func() {
			select {
			case r.serveDoneCh <- scheduler.ServeDoneEvent{ModelID: modelID}:
			case <-r.shutdownCtx.Done():
			}
		}()
		rt.ServeHTTP(w, req)
	}
}

func (r *Router) ServeModel(model string, w http.ResponseWriter, req *http.Request) {
	if r.shutting.Load() {
		writeErr(w, scheduler.ErrShutdown)
		return
	}
	hr := scheduler.HandlerReq{
		ID:      r.nextReq.Add(1),
		Model:   model,
		Ctx:     req.Context(),
		Admit:   make(chan error, 1),
		Respond: make(chan scheduler.HandlerResp),
	}
	select {
	case r.handlerCh <- hr:
	case <-req.Context().Done():
		return
	case <-r.shutdownCtx.Done():
		writeErr(w, scheduler.ErrShutdown)
		return
	}

	select {
	case err := <-hr.Admit:
		if err != nil {
			writeErr(w, err)
			return
		}
	case <-req.Context().Done():
		select {
		case r.cancelCh <- hr:
		case <-r.shutdownCtx.Done():
		}
		return
	case <-r.shutdownCtx.Done():
		writeErr(w, scheduler.ErrShutdown)
		return
	}

	go r.watchQueueTimeout(hr)

	select {
	case resp := <-hr.Respond:
		if resp.Err != nil {
			writeErr(w, resp.Err)
			return
		}
		if resp.HandleFunc != nil {
			resp.HandleFunc(w, req)
		}
	case <-req.Context().Done():
		select {
		case r.cancelCh <- hr:
		case <-r.shutdownCtx.Done():
		}
		return
	case <-r.shutdownCtx.Done():
		writeErr(w, scheduler.ErrShutdown)
		return
	}
}

func (r *Router) watchQueueTimeout(hr scheduler.HandlerReq) {
	qto := 180 * time.Second
	if r.cfg != nil && r.cfg.QueueTimeout > 0 {
		qto = r.cfg.QueueTimeout
	}
	select {
	case <-r.clock.After(qto):
		select {
		case r.timeoutCh <- hr:
		case <-r.shutdownCtx.Done():
		}
	case <-hr.Ctx.Done():
	case <-r.shutdownCtx.Done():
	}
}

func (r *Router) Reconcile(ctx context.Context) error {
	for id, rt := range r.runtimes {
		if err := rt.Reconcile(ctx); err != nil {
			return fmt.Errorf("reconcile %s: %w", id, err)
		}
	}
	return nil
}

func (r *Router) Preload(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if _, ok := r.runtimes[id]; !ok {
			return fmt.Errorf("preload unknown model %s", id)
		}
		if err := r.prepareModel(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) prepareModel(ctx context.Context, model string) error {
	hr := scheduler.HandlerReq{
		ID:      r.nextReq.Add(1),
		Model:   model,
		Ctx:     ctx,
		Admit:   make(chan error, 1),
		Respond: make(chan scheduler.HandlerResp),
	}
	select {
	case r.handlerCh <- hr:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCtx.Done():
		return scheduler.ErrShutdown
	}
	select {
	case err := <-hr.Admit:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		select {
		case r.cancelCh <- hr:
		default:
		}
		return ctx.Err()
	}
	select {
	case resp := <-hr.Respond:
		if resp.Err != nil {
			return resp.Err
		}
		select {
		case r.cancelGrantedCh <- scheduler.CancelGrantedEvent{ModelID: model}:
		case <-r.shutdownCtx.Done():
		}
		return nil
	case <-ctx.Done():
		select {
		case r.cancelCh <- hr:
		default:
		}
		return ctx.Err()
	}
}

func (r *Router) Remaining() []string {
	var left []string
	for id, rt := range r.runtimes {
		st := rt.State()
		if st != runtime.StateStopped && st != runtime.StateShutdown {
			left = append(left, id)
		}
	}
	return left
}

func (r *Router) Unconfirmed() []string {
	out := append([]string{}, r.unconfirmed...)
	return out
}

func (r *Router) Shutdown(ctx context.Context) error {
	r.shutting.Store(true)
	done := make(chan struct{})
	select {
	case r.shutdownCh <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	var failed []string
	for id, rt := range r.runtimes {
		if err := r.releaseOnShutdown(ctx, id, rt); err != nil {
			failed = append(failed, id)
		}
	}
	sort.Strings(failed)
	r.onLoop(func() { r.unconfirmed = append([]string{}, failed...) })
	r.shutdownFn()
	select {
	case <-r.runDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, rt := range r.runtimes {
		rt.Shutdown()
	}
	if len(failed) > 0 {
		return fmt.Errorf("unconfirmed models: %s", strings.Join(failed, ","))
	}
	return nil
}

func (r *Router) releaseOnShutdown(ctx context.Context, id string, rt runtime.Runtime) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(r.drainBudget())
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var protected bool
		r.onLoop(func() { protected = r.schedule.Protected(id) })
		if !protected {
			break
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%s still executing", id)
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	switch rt.State() {
	case runtime.StateStopped, runtime.StateShutdown:
		return nil
	case runtime.StateUnknown, runtime.StateFailed, runtime.StateStarting, runtime.StateStopping:
		return fmt.Errorf("%s state %s", id, rt.State())
	}
	if err := r.waitIdle(rt, deadline); err != nil {
		return err
	}
	to := time.Second
	if r.cfg != nil && r.cfg.ShutdownTimeout > 0 {
		to = r.cfg.ShutdownTimeout
	}
	if err := rt.Stop(ctx, to); err != nil {
		return err
	}
	at := r.clock.Now()
	snap, err := r.sampleAfter(at)
	if err != nil {
		return err
	}
	r.onLoop(func() {
		r.residualAfter[id] = at
		r.applyObservation(snap)
	})
	return nil
}

func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	code := "BACKEND"
	msg := err.Error()
	var se *scheduler.Error
	if errors.As(err, &se) {
		status = se.Status
		code = se.Code
		msg = se.Msg
	}
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "1")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": msg,
		"src":   "inferswap",
		"code":  code,
	})
}

func (r *Router) NotifyBackendDone(modelID string) {
	select {
	case r.backendDoneCh <- scheduler.BackendDoneEvent{ModelID: modelID}:
	case <-r.shutdownCtx.Done():
	}
}
