package scheduler

import (
	"context"
	"net/http"

	"github.com/kilingki/InferSwap/internal/runtime"
)

type Swapper interface {
	EvictionFor(target string, running []string) []string
}

type Scheduler interface {
	OnRequest(req HandlerReq)
	OnCancel(req HandlerReq)
	OnUnloadDone(ev UnloadDone)
	OnSwapDone(ev SwapDone)
	OnServeDone(ev ServeDoneEvent)
	OnProxyStart(ev ProxyStartEvent)
	OnCancelGranted(ev CancelGrantedEvent)
	OnStatus(ev StatusEvent)
	OnBackendDone(ev BackendDoneEvent)
	OnQueueTimeout(req HandlerReq)
	OnShutdown(err error)
}

type Effects interface {
	ModelState(modelID string) (runtime.State, bool)
	RunningModels() map[string]runtime.State
	LastStatus(modelID string) (runtime.Status, bool)
	StartUnload(ids []string)
	StartLoad(modelID string)
	GrantError(req HandlerReq, err error)
	GrantServe(req HandlerReq, modelID string) bool
}

type HandlerReq struct {
	ID      uint64
	Model   string
	Ctx     context.Context
	Admit   chan error
	Respond chan HandlerResp
}

type HandlerResp struct {
	HandleFunc http.HandlerFunc
	Err        error
}

type SwapDone struct {
	ModelID string
	Err     error
}

type UnloadDone struct {
	IDs []string
	Err error
}

type ServeDoneEvent struct {
	ModelID string
}

type ProxyStartEvent struct {
	ModelID string
	OK      chan bool
}

type CancelGrantedEvent struct {
	ModelID string
}

type StatusEvent struct {
	ModelID string
	Status  runtime.Status
	Err     error
}

type BackendDoneEvent struct {
	ModelID string
}
