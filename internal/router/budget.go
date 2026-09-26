package router

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/resource"
	"github.com/kilingki/InferSwap/internal/router/scheduler"
	"github.com/kilingki/InferSwap/internal/runtime"
)

func (r *Router) EvictionFor(target string, running []string) []string {
	evict, err := r.Decide(target, running)
	if err != nil {
		return nil
	}
	return evict
}

func (r *Router) Decide(target string, running []string) ([]string, error) {
	st, ok := r.ModelState(target)
	if ok && st == runtime.StateReady {
		return nil, nil
	}
	if !r.gpuFresh() {
		return nil, scheduler.ErrNotBooted
	}
	if r.fits(target) {
		return nil, nil
	}
	if !r.fitsIfRestResidual(target) {
		return nil, scheduler.ErrResources
	}
	victim := r.oldestResident(running, target)
	if victim == "" {
		return nil, scheduler.ErrResources
	}
	return []string{victim}, nil
}

func (r *Router) WaitLoad(ctx context.Context, model string) error {
	resp := make(chan error, 1)
	select {
	case r.admitCh <- admitReq{model: model, resp: resp}:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCtx.Done():
		return scheduler.ErrShutdown
	}
	select {
	case err := <-resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.shutdownCtx.Done():
		return scheduler.ErrShutdown
	}
}

func (r *Router) grantLoad(id string) error {
	if err := r.admitLoad(id); err != nil {
		slog.Info("load admission denied", "model", id, "err", err)
		return err
	}
	slog.Info("load admitted", "model", id)
	return nil
}

func (r *Router) admitLoad(id string) error {
	if r.shutting.Load() {
		return scheduler.ErrShutdown
	}
	if !r.gpuFresh() {
		return scheduler.ErrNotBooted
	}
	if r.loadSerial != "" && r.loadSerial != id {
		return scheduler.ErrResources
	}
	if !r.fits(id) {
		return scheduler.ErrResources
	}
	m, ok := r.cfg.Models[id]
	if !ok {
		return scheduler.ErrModelNotFound
	}
	r.reserved[id] = m.LoadBound()
	r.loadSerial = id
	return nil
}

func (r *Router) gpuFresh() bool {
	if !r.bootGPU {
		return false
	}
	return resource.Fresh(r.snap, time.Now(), r.observeAge())
}

func (r *Router) observeAge() time.Duration {
	if r.cfg != nil && r.cfg.GPU.MaxObservationAge > 0 {
		return r.cfg.GPU.MaxObservationAge
	}
	return 5 * time.Second
}

func (r *Router) margin() int64 {
	if r.cfg == nil {
		return 0
	}
	return r.cfg.GPU.SafetyMarginBytes
}

func (r *Router) fits(target string) bool {
	free := int64(r.snap.Free)
	var holds []resource.Hold
	for id, m := range r.cfg.Models {
		bound, ok := r.boundFor(id, m, id == target)
		if !ok {
			return false
		}
		holds = append(holds, resource.Hold{ID: id, Bound: bound})
	}
	return resource.Fits(free, r.margin(), holds)
}

func (r *Router) fitsIfRestResidual(target string) bool {
	var holds []resource.Hold
	for id, m := range r.cfg.Models {
		if id == target {
			holds = append(holds, resource.Hold{ID: id, Bound: m.LoadBound()})
			continue
		}
		st, ok := r.ModelState(id)
		if !ok || st == runtime.StateUnknown || st == runtime.StateFailed || st == runtime.StateShutdown {
			return false
		}
		holds = append(holds, resource.Hold{ID: id, Bound: m.ResourceProfile.UnloadedResidualBytes})
	}
	// Capacity is the device total. Current free still includes the resident
	// model, so it cannot answer whether the target fits after that model is residual.
	return resource.Fits(int64(r.snap.Total), r.margin(), holds)
}

func (r *Router) boundFor(id string, m config.Model, asLoad bool) (int64, bool) {
	if asLoad {
		return m.LoadBound(), true
	}
	if b, ok := r.reserved[id]; ok {
		return b, true
	}
	st, ok := r.ModelState(id)
	if !ok {
		return 0, false
	}
	switch st {
	case runtime.StateReady, runtime.StateStarting, runtime.StateStopping:
		return m.LoadBound(), true
	case runtime.StateStopped:
		return m.ResourceProfile.UnloadedResidualBytes, true
	default:
		return 0, false
	}
}

func (r *Router) oldestResident(running []string, target string) string {
	type cand struct {
		id string
		at time.Time
	}
	var cs []cand
	for _, id := range running {
		if id == target {
			continue
		}
		st, _ := r.ModelState(id)
		if st != runtime.StateReady {
			continue
		}
		at := r.lastUse[id]
		if at.IsZero() {
			at = r.loadedAt[id]
		}
		cs = append(cs, cand{id: id, at: at})
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].at.Equal(cs[j].at) {
			return cs[i].id < cs[j].id
		}
		return cs[i].at.Before(cs[j].at)
	})
	if len(cs) == 0 {
		return ""
	}
	return cs[0].id
}

func (r *Router) noteUse(id string) {
	r.lastUse[id] = r.clock.Now()
}

func (r *Router) noteLoaded(id string) {
	now := r.clock.Now()
	r.loadedAt[id] = now
	if r.lastUse[id].IsZero() {
		r.lastUse[id] = now
	}
	if m, ok := r.cfg.Models[id]; ok {
		r.reserved[id] = m.LoadBound()
	}
}

func (r *Router) holdResidual(id string) {
	m, ok := r.cfg.Models[id]
	if !ok {
		return
	}
	r.reserved[id] = m.ResourceProfile.UnloadedResidualBytes
}
