package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func ParseStatus(body []byte) (Status, error) {
	if !json.Valid(body) {
		return Status{}, fmt.Errorf("status: malformed json")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return Status{}, fmt.Errorf("status: malformed json")
	}
	for _, key := range []string{"state", "residency", "active_requests", "last_error"} {
		if _, ok := raw[key]; !ok {
			return Status{}, fmt.Errorf("status: missing %s", key)
		}
	}
	state, err := decodeString(raw["state"], "state")
	if err != nil {
		return Status{}, err
	}
	residency, err := decodeString(raw["residency"], "residency")
	if err != nil {
		return Status{}, err
	}
	active, err := decodeActive(raw["active_requests"])
	if err != nil {
		return Status{}, err
	}
	last, err := decodeLastError(raw["last_error"])
	if err != nil {
		return Status{}, err
	}
	st := Status{
		State:          WireState(state),
		Residency:      Residency(residency),
		ActiveRequests: active,
		LastError:      last,
	}
	if err := validateStatus(st); err != nil {
		return Status{}, err
	}
	return st, nil
}

func validateStatus(st Status) error {
	switch st.State {
	case WireUnloaded, WireLoading, WireReady, WireUnloading, WireFailed:
	default:
		return fmt.Errorf("status: invalid state %q", st.State)
	}
	switch st.Residency {
	case ResidencyResident, ResidencyNotResident, ResidencyUnknown:
	default:
		return fmt.Errorf("status: invalid residency %q", st.Residency)
	}
	if st.State == WireReady && st.Residency != ResidencyResident {
		return fmt.Errorf("status: ready requires residency=resident")
	}
	if st.State == WireUnloaded && (st.Residency != ResidencyNotResident || st.ActiveRequests != 0) {
		return fmt.Errorf("status: unloaded requires not_resident and active_requests=0")
	}
	return nil
}

func decodeString(raw json.RawMessage, field string) (string, error) {
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) || len(raw) == 0 {
		return "", fmt.Errorf("status: null %s", field)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("status: %s: %w", field, err)
	}
	if s == "" {
		return "", fmt.Errorf("status: empty %s", field)
	}
	return s, nil
}

func decodeActive(raw json.RawMessage) (int, error) {
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) || len(raw) == 0 {
		return 0, fmt.Errorf("status: null active_requests")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var n json.Number
	if err := dec.Decode(&n); err != nil {
		return 0, fmt.Errorf("status: active_requests: %w", err)
	}
	i, err := n.Int64()
	if err != nil {
		return 0, fmt.Errorf("status: active_requests must be an integer")
	}
	if i < 0 {
		return 0, fmt.Errorf("status: negative active_requests")
	}
	return int(i), nil
}

func decodeLastError(raw json.RawMessage) (*LastError, error) {
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("status: last_error: %w", err)
	}
	codeRaw, ok := fields["code"]
	if !ok {
		return nil, fmt.Errorf("status: last_error missing code")
	}
	msgRaw, ok := fields["message"]
	if !ok {
		return nil, fmt.Errorf("status: last_error missing message")
	}
	var code, message string
	if err := json.Unmarshal(codeRaw, &code); err != nil || code == "" {
		return nil, fmt.Errorf("status: last_error.code")
	}
	if err := json.Unmarshal(msgRaw, &message); err != nil {
		return nil, fmt.Errorf("status: last_error.message")
	}
	return &LastError{Code: code, Message: message}, nil
}

func ParseControlError(body []byte) (string, string, error) {
	if !json.Valid(body) {
		return "", "", fmt.Errorf("control error: malformed json")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return "", "", fmt.Errorf("control error: malformed json")
	}
	errRaw, ok := raw["error"]
	if !ok {
		return "", "", fmt.Errorf("control error: missing error")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(errRaw, &fields); err != nil {
		return "", "", fmt.Errorf("control error: %w", err)
	}
	codeRaw, ok := fields["code"]
	if !ok {
		return "", "", fmt.Errorf("control error: missing error.code")
	}
	msgRaw, ok := fields["message"]
	if !ok {
		return "", "", fmt.Errorf("control error: missing error.message")
	}
	var code, message string
	if err := json.Unmarshal(codeRaw, &code); err != nil || code == "" {
		return "", "", fmt.Errorf("control error: error.code")
	}
	if err := json.Unmarshal(msgRaw, &message); err != nil {
		return "", "", fmt.Errorf("control error: error.message")
	}
	return code, message, nil
}
