package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
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
}

func NewExclusive(cfg *config.Config, runtimes map[string]runtime.Runtime, clock testkit.Clock) *Router {
	if clock == nil {
		clock = testkit.RealClock{}
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
	}
	r.schedule = scheduler.NewFIFO(cfg, exclusiveSwapper{}, r)
	go r.run()
	return r
}

func (r *Router) run() {
	defer close(r.runDone)
	for {
		select {
		case done := <-r.shutdownCh:
			r.schedule.OnShutdown(scheduler.ErrShutdown)
			if done != nil {
				close(done)
			}
			return
		case req := <-r.handlerCh:
			r.schedule.OnRequest(req)
		case req := <-r.cancelCh:
			r.schedule.OnCancel(req)
		case ev := <-r.unloadDoneCh:
			r.schedule.OnUnloadDone(ev)
		case ev := <-r.swapDoneCh:
			r.schedule.OnSwapDone(ev)
		case ev := <-r.serveDoneCh:
			r.schedule.OnServeDone(ev)
			go r.refreshStatus(ev.ModelID)
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
		}
	}
}

func (r *Router) refreshStatus(id string) {
	rt, ok := r.runtimes[id]
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.statusTimeout())
	defer cancel()
	st, err := rt.Status(ctx)
	select {
	case r.statusCh <- scheduler.StatusEvent{ModelID: id, Status: st, Err: err}:
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

func (r *Router) StartUnload(ids []string) {
	go func() {
		var first error
		for _, id := range ids {
			rt, ok := r.runtimes[id]
			if !ok {
				continue
			}
			to := time.Second
			if m, ok := r.cfg.Models[id]; ok && m.UnloadTimeout > 0 {
				to = m.UnloadTimeout
			} else if r.cfg != nil && r.cfg.UnloadTimeout > 0 {
				to = r.cfg.UnloadTimeout
			}
			if err := rt.Stop(context.Background(), to); err != nil && first == nil {
				first = err
			}
		}
		select {
		case r.unloadDoneCh <- scheduler.UnloadDone{IDs: ids, Err: first}:
		case <-r.shutdownCtx.Done():
		}
	}()
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

func (r *Router) Shutdown(ctx context.Context) error {
	r.shutting.Store(true)
	r.shutdownFn()
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
	var wg sync.WaitGroup
	for _, rt := range r.runtimes {
		wg.Add(1)
		go func(rt runtime.Runtime) {
			defer wg.Done()
			to := time.Second
			if r.cfg != nil && r.cfg.ShutdownTimeout > 0 {
				to = r.cfg.ShutdownTimeout
			}
			_ = rt.Stop(context.Background(), to)
			rt.Shutdown()
		}(rt)
	}
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
