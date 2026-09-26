package mock

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeControlError(w, http.StatusMethodNotAllowed, "GET required", "BAD_REQUEST")
		return
	}
	s.mu.Lock()
	mode := s.statusMode
	st := s.statusLocked()
	s.mu.Unlock()
	switch mode {
	case StatusMalformed:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"state":`))
		return
	case StatusUnreliable:
		writeControlError(w, http.StatusInternalServerError, "status unavailable", "STATUS_FAILED")
		return
	case StatusNullActive:
		writeJSON(w, http.StatusOK, map[string]any{
			"state":           "unloaded",
			"residency":       "not_resident",
			"active_requests": nil,
			"last_error":      nil,
		})
		return
	case StatusNegativeActive:
		writeJSON(w, http.StatusOK, map[string]any{
			"state":           "ready",
			"residency":       "resident",
			"active_requests": -1,
			"last_error":      nil,
		})
		return
	case StatusMissingActive:
		writeJSON(w, http.StatusOK, map[string]any{
			"state":      "unloaded",
			"residency":  "not_resident",
			"last_error": nil,
		})
		return
	case StatusReadyNotResident:
		writeJSON(w, http.StatusOK, map[string]any{
			"state":           "ready",
			"residency":       "not_resident",
			"active_requests": 0,
			"last_error":      nil,
		})
		return
	default:
		writeJSON(w, http.StatusOK, st)
	}
}

func (s *Server) handleLoad(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeControlError(w, http.StatusMethodNotAllowed, "POST required", "BAD_REQUEST")
		return
	}
	if err := decodeEmptyObject(r.Body); err != nil {
		writeControlError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
		return
	}

	s.mu.Lock()
	switch s.modelState {
	case StateReady:
		st := s.statusLocked()
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, st)
		return
	case StateUnloading:
		s.mu.Unlock()
		writeControlError(w, http.StatusConflict, "unload in progress", "LIFECYCLE_CONFLICT")
		return
	case StateFailed:
		if s.residency != ResidencyNotResident {
			s.mu.Unlock()
			writeControlError(w, http.StatusConflict, "residency not clear", "LIFECYCLE_CONFLICT")
			return
		}
	case StateLoading:
		if s.loadCh != nil {
			wait := s.loadCh
			s.mu.Unlock()
			s.waitAndWriteLoad(w, r, wait)
			return
		}
	}
	if s.loadCh != nil {
		wait := s.loadCh
		s.mu.Unlock()
		s.waitAndWriteLoad(w, r, wait)
		return
	}
	s.counts.LoadStarts++
	s.modelState = StateLoading
	s.residency = ResidencyUnknown
	ch := make(chan struct{})
	s.loadCh = ch
	gate := s.loadGate
	fail := s.loadFail
	s.mu.Unlock()

	go s.runLoad(ch, gate, fail)
	s.waitAndWriteLoad(w, r, ch)
}

func (s *Server) runLoad(done chan struct{}, gate interface{ Hit(context.Context) error }, fail LoadFailMode) {
	defer close(done)
	if gate != nil {
		_ = gate.Hit(context.Background())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.LoadEnds++
	s.loadCh = nil
	switch fail {
	case LoadFailNotResident:
		s.modelState = StateFailed
		s.residency = ResidencyNotResident
		s.active = 0
		s.lastError = &LastError{Code: "LOAD_FAILED", Message: "load failed"}
		return
	case LoadFailUnknown:
		s.modelState = StateFailed
		s.residency = ResidencyUnknown
		s.active = 0
		s.lastError = &LastError{Code: "LOAD_FAILED", Message: "load failed after partial GPU allocation"}
		return
	}
	s.modelState = StateReady
	s.residency = ResidencyResident
	s.lastError = nil
}

func (s *Server) waitAndWriteLoad(w http.ResponseWriter, r *http.Request, wait <-chan struct{}) {
	select {
	case <-wait:
		s.mu.Lock()
		fail := s.modelState == StateFailed
		st := s.statusLocked()
		s.mu.Unlock()
		if fail {
			writeControlError(w, http.StatusInternalServerError, "load failed", "LOAD_FAILED")
			return
		}
		writeJSON(w, http.StatusOK, st)
	case <-r.Context().Done():
		return
	}
}

func (s *Server) handleUnload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeControlError(w, http.StatusMethodNotAllowed, "POST required", "BAD_REQUEST")
		return
	}
	if err := decodeEmptyObject(r.Body); err != nil {
		writeControlError(w, http.StatusBadRequest, err.Error(), "BAD_REQUEST")
		return
	}

	s.mu.Lock()
	if s.modelState == StateLoading {
		s.mu.Unlock()
		writeControlError(w, http.StatusConflict, "load in progress", "LIFECYCLE_CONFLICT")
		return
	}
	if s.busyLocked() {
		s.mu.Unlock()
		writeControlError(w, http.StatusConflict, "runtime has active inference requests", "BUSY")
		return
	}
	if s.modelState == StateUnloaded && s.residency == ResidencyNotResident {
		st := s.statusLocked()
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, st)
		return
	}
	if s.modelState == StateFailed && s.residency == ResidencyNotResident {
		s.modelState = StateUnloaded
		s.residency = ResidencyNotResident
		s.active = 0
		s.lastError = nil
		st := s.statusLocked()
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, st)
		return
	}
	if s.unloadCh != nil {
		wait := s.unloadCh
		s.mu.Unlock()
		s.waitAndWriteUnload(w, r, wait)
		return
	}
	s.counts.UnloadStarts++
	s.modelState = StateUnloading
	ch := make(chan struct{})
	s.unloadCh = ch
	gate := s.unloadGate
	fail := s.unloadFail
	s.mu.Unlock()

	go s.runUnload(ch, gate, fail)
	s.waitAndWriteUnload(w, r, ch)
}

func (s *Server) runUnload(done chan struct{}, gate interface{ Hit(context.Context) error }, fail bool) {
	defer close(done)
	if gate != nil {
		_ = gate.Hit(context.Background())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.UnloadEnds++
	s.unloadCh = nil
	if s.busyLocked() {
		s.modelState = StateReady
		s.residency = ResidencyResident
		s.lastError = &LastError{Code: "BUSY", Message: "runtime has active inference requests"}
		return
	}
	if fail {
		s.modelState = StateFailed
		s.residency = ResidencyResident
		s.lastError = &LastError{Code: "UNLOAD_FAILED", Message: "failed to release model"}
		return
	}
	s.modelState = StateUnloaded
	s.residency = ResidencyNotResident
	s.active = 0
	s.lastError = nil
}

func (s *Server) waitAndWriteUnload(w http.ResponseWriter, r *http.Request, wait <-chan struct{}) {
	select {
	case <-wait:
		s.mu.Lock()
		busy := s.lastError != nil && s.lastError.Code == "BUSY"
		fail := s.modelState == StateFailed
		st := s.statusLocked()
		s.mu.Unlock()
		if busy {
			writeControlError(w, http.StatusConflict, "runtime has active inference requests", "BUSY")
			return
		}
		if fail {
			writeControlError(w, http.StatusInternalServerError, "unload failed", "UNLOAD_FAILED")
			return
		}
		writeJSON(w, http.StatusOK, st)
	case <-r.Context().Done():
		return
	}
}

func (s *Server) handleInference(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/v1/") && r.URL.Path != "/align" {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	s.lastPath = r.URL.RequestURI()
	if s.modelState != StateReady || s.residency != ResidencyResident {
		s.mu.Unlock()
		writeControlError(w, http.StatusServiceUnavailable, "not ready", "NOT_READY")
		return
	}
	s.counts.InferStarts++
	s.addActiveLocked(1)
	gate := s.inferGate
	backend := s.backendGate
	persist := s.persistInfer
	s.mu.Unlock()

	if gate != nil {
		_ = gate.Hit(context.Background())
	}

	disconnected := r.Context().Err() != nil
	if persist && disconnected {
		go s.finishInference(backend)
		return
	}

	if persist && backend != nil {
		if err := backend.Hit(r.Context()); err != nil {
			go s.finishInference(backend)
			return
		}
	}

	s.finishInference(nil)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":      "mock",
		"object":  "chat.completion",
		"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": s.name}}},
	})
}

func (s *Server) finishInference(backend interface{ Hit(context.Context) error }) {
	if backend != nil {
		_ = backend.Hit(context.Background())
	}
	s.mu.Lock()
	s.addActiveLocked(-1)
	s.counts.InferEnds++
	s.mu.Unlock()
}
