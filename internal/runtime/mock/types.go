package mock

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

const ProtocolVersion = "v0"

type ModelState string

const (
	StateUnloaded  ModelState = "unloaded"
	StateLoading   ModelState = "loading"
	StateReady     ModelState = "ready"
	StateUnloading ModelState = "unloading"
	StateFailed    ModelState = "failed"
	StateUnknown   ModelState = "unknown"
)

type Status struct {
	ProtocolVersion  string     `json:"protocol_version"`
	EnvironmentReady bool       `json:"environment_ready"`
	ModelState       ModelState `json:"model_state"`
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

type ControlError struct {
	Error string `json:"error"`
	Src   string `json:"src"`
	Code  string `json:"code"`
}

type Counts struct {
	PrepareStarts int
	PrepareEnds   int
	LoadStarts    int
	LoadEnds      int
	UnloadStarts  int
	UnloadEnds    int
	InferStarts   int
	InferEnds     int
	HealthChecks  int
}

type StatusMode int

const (
	StatusNormal StatusMode = iota
	StatusUnknown
	StatusMalformed
	StatusTransportError
)

type GPUSnapshot struct {
	FreeBytes int64
	At        time.Time
	Err       error
}

func boolPtr(v bool) *bool { return &v }
func intPtr(v int) *int    { return &v }

type constError string

func (e constError) Error() string { return string(e) }

const errNonEmptyBody constError = "request body must be {}"

func decodeEmptyObject(r io.Reader) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return err
	}
	if len(obj) != 0 {
		return errNonEmptyBody
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeControlError(w http.ResponseWriter, status int, message, code string) {
	writeJSON(w, status, ControlError{Error: message, Src: "runtime", Code: code})
}
