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

type Runtime interface {
	EnsureReady(ctx context.Context, timeout time.Duration) error
	Stop(ctx context.Context, timeout time.Duration) error
	Status(ctx context.Context) (Status, error)
	State() State
	ServeHTTP(http.ResponseWriter, *http.Request)
	Reconcile(ctx context.Context) error
	Shutdown()
}

type Status struct {
	ProtocolVersion  string     `json:"protocol_version"`
	EnvironmentReady bool       `json:"environment_ready"`
	ModelState       string     `json:"model_state"`
	InferenceReady   bool       `json:"inference_ready"`
	Resident         *bool      `json:"resident"`
	ActiveRequests   *int       `json:"active_requests"`
	Resource         Resource   `json:"resource"`
	LastError        *LastError `json:"last_error"`
}

type Resource struct {
	DeviceID              *string `json:"device_id"`
	ObservedBytes         *int64  `json:"observed_bytes"`
	UnloadedResidualBytes *int64  `json:"unloaded_residual_bytes"`
	Budget                any     `json:"budget"`
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

var (
	ErrShutdown           = &CodeError{Code: "SHUTDOWN", Msg: "runtime is shutdown"}
	ErrPrepareUnavailable = &CodeError{Code: "PREPARE_UNAVAILABLE", Msg: "endpoint is down and no prepare command is registered"}
	ErrNotReady           = &CodeError{Code: "NOT_READY", Msg: "runtime is not ready"}
)
