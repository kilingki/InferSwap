package runtime

import (
	"context"
	"net/http"
	"time"
)

type State string

const (
	StateUnknown  State = "unknown"
	StateStopped  State = "stopped"
	StateStarting State = "starting"
	StateReady    State = "ready"
	StateStopping State = "stopping"
	StateFailed   State = "failed"
	StateShutdown State = "shutdown"
)

type WireState string

const (
	WireUnloaded  WireState = "unloaded"
	WireLoading   WireState = "loading"
	WireReady     WireState = "ready"
	WireUnloading WireState = "unloading"
	WireFailed    WireState = "failed"
)

type Residency string

const (
	ResidencyResident    Residency = "resident"
	ResidencyNotResident Residency = "not_resident"
	ResidencyUnknown     Residency = "unknown"
)

type Runtime interface {
	EnsureReady(ctx context.Context, timeout time.Duration) error
	Stop(ctx context.Context, timeout time.Duration) error
	Status(ctx context.Context) (Status, error)
	State() State
	ServeHTTP(http.ResponseWriter, *http.Request)
	Reconcile(ctx context.Context) error
	Shutdown()
}

// Status is the final control status. Local State is not this wire value.
type Status struct {
	State          WireState
	Residency      Residency
	ActiveRequests int
	LastError      *LastError
}

type LastError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type CodeError struct {
	Code string
	Msg  string
}

func (e *CodeError) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return e.Code
}

// ControlFault is a non-200 control response. Flow uses Code, not Message.
type ControlFault struct {
	Status  int
	Code    string
	Message string
}

func (e *ControlFault) Error() string {
	if e.Code != "" {
		return e.Code
	}
	return "control error"
}

var (
	ErrShutdown           = &CodeError{Code: "SHUTDOWN", Msg: "runtime is shutdown"}
	ErrPrepareUnavailable = &CodeError{Code: "PREPARE_UNAVAILABLE", Msg: "endpoint is down and no prepare command is registered"}
	ErrNotReady           = &CodeError{Code: "NOT_READY", Msg: "runtime is not ready"}
	ErrContract           = &CodeError{Code: "CONTRACT", Msg: "runtime status violated the control contract"}
)
