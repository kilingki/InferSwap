package scheduler

import "net/http"

type Error struct {
	Status int
	Code   string
	Msg    string
}

func (e *Error) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return e.Code
}

func (e *Error) StatusCode() int { return e.Status }

var (
	ErrModelNotFound = &Error{Status: http.StatusNotFound, Code: "NOT_FOUND", Msg: "model not found"}
	ErrQueueFull     = &Error{Status: http.StatusTooManyRequests, Code: "QUEUE_FULL", Msg: "queue is full"}
	ErrQueueTimeout  = &Error{Status: http.StatusGatewayTimeout, Code: "QUEUE_TIMEOUT", Msg: "queue timeout"}
	ErrDrainTimeout  = &Error{Status: http.StatusGatewayTimeout, Code: "DRAIN_TIMEOUT", Msg: "drain timed out"}
	ErrUnloadBlocked = &Error{Status: http.StatusBadGateway, Code: "UNLOAD_FAILED", Msg: "unload failed; next load blocked"}
	ErrShutdown      = &Error{Status: http.StatusServiceUnavailable, Code: "SHUTDOWN", Msg: "router is shutting down"}
	ErrLoadFailed    = &Error{Status: http.StatusBadGateway, Code: "LOAD_FAILED", Msg: "load failed"}
	ErrResources    = &Error{Status: http.StatusServiceUnavailable, Code: "RESOURCES", Msg: "gpu budget is not available"}
	ErrNotBooted    = &Error{Status: http.StatusServiceUnavailable, Code: "NOT_READY", Msg: "gpu observation is not ready"}
)
