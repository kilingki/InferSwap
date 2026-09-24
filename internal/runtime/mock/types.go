package mock

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

type WireState string

const (
	StateUnloaded  WireState = "unloaded"
	StateLoading   WireState = "loading"
	StateReady     WireState = "ready"
	StateUnloading WireState = "unloading"
	StateFailed    WireState = "failed"
)

type Residency string

const (
	ResidencyResident    Residency = "resident"
	ResidencyNotResident Residency = "not_resident"
	ResidencyUnknown     Residency = "unknown"
)

type Status struct {
	State          WireState  `json:"state"`
	Residency      Residency  `json:"residency"`
	ActiveRequests int        `json:"active_requests"`
	LastError      *LastError `json:"last_error"`
}

type LastError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type controlErrorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
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
}

type StatusMode int

const (
	StatusNormal StatusMode = iota
	StatusMalformed
	StatusTransportError
	StatusUnreliable
	StatusNullActive
	StatusNegativeActive
	StatusReadyNotResident
	StatusMissingActive
)

type LoadFailMode int

const (
	LoadOK LoadFailMode = iota
	LoadFailNotResident
	LoadFailUnknown
)

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
	var body controlErrorBody
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(w, status, body)
}
