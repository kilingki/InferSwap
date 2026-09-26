package router

import (
	"time"

	"github.com/kilingki/InferSwap/internal/runtime"
)

type Diagnostics struct {
	QueueDepth int
	GPU        GPUDiagnostics
	Models     map[string]ModelDiagnostics
}

type GPUDiagnostics struct {
	ObservedAt time.Time
	Fresh      bool
	Total      uint64
	Free       uint64
}

type ModelDiagnostics struct {
	ReservedBytes int64
	State         runtime.State
	Reason        string
	LastError     *runtime.LastError
}

func (r *Router) Diagnostics() Diagnostics {
	out := Diagnostics{Models: map[string]ModelDiagnostics{}}
	r.onLoop(func() {
		out = r.snapshotDiagnostics()
	})
	return out
}

func (r *Router) snapshotDiagnostics() Diagnostics {
	models := make(map[string]ModelDiagnostics, len(r.runtimes))
	for id, rt := range r.runtimes {
		item := ModelDiagnostics{
			ReservedBytes: r.reserved[id],
			State:         rt.State(),
		}
		if c, ok := rt.(*runtime.Client); ok {
			item.Reason = c.StateReason()
		}
		if st, ok := r.lastStatus[id]; ok && st.LastError != nil {
			cp := *st.LastError
			item.LastError = &cp
		}
		models[id] = item
	}
	return Diagnostics{
		QueueDepth: r.schedule.QueueDepth(),
		GPU: GPUDiagnostics{
			ObservedAt: r.snap.ObservedAt,
			Fresh:      r.gpuFresh(),
			Total:      r.snap.Total,
			Free:       r.snap.Free,
		},
		Models: models,
	}
}
