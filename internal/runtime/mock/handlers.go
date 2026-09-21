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
		_, _ = w.Write([]byte(`{"protocol_version":`))
		return
	case StatusUnknown:
		st.ModelState = StateUnknown
		st.Resident = nil
		st.ActiveRequests = nil
		st.InferenceReady = false
		writeJSON(w, http.StatusOK, st)
		return
	default:
		writeJSON(w, http.StatusOK, st)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeControlError(w, http.StatusMethodNotAllowed, "GET required", "BAD_REQUEST")
		return
	}
	s.mu.Lock()
	s.counts.HealthChecks++
	ready := s.inferenceReady && !s.healthHeld
	s.mu.Unlock()
	if !ready {
		writeControlError(w, http.StatusServiceUnavailable, "not ready", "NOT_READY")
		return
	}
	w.WriteHeader(http.StatusOK)
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
	if s.modelState == StateReady && s.inferenceReady && !s.healthHeld {
		st := s.statusLocked()
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, st)
		return
	}
	if s.loadCh != nil {
		wait := s.loadCh
		s.mu.Unlock()
		s.waitAndWriteLoad(w, r, wait)
		return
	}
	s.counts.LoadStarts++
	s.modelState = StateLoading
	s.inferenceReady = false
	ch := make(chan struct{})
	s.loadCh = ch
	gate := s.loadGate
	healthGate := s.healthGate
	fail := s.loadFail
	held := s.healthHeld
	s.mu.Unlock()

	go s.runLoad(ch, gate, healthGate, fail, held)
	s.waitAndWriteLoad(w, r, ch)
}

func (s *Server) runLoad(done chan struct{}, gate, healthGate interface{ Hit(context.Context) error }, fail, healthHeld bool) {
	defer close(done)
	if gate != nil {
		_ = gate.Hit(context.Background())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.LoadEnds++
	s.loadCh = nil
	if fail {
		s.modelState = StateFailed
		s.inferenceReady = false
		s.resident = boolPtr(false)
		s.lastError = &LastError{Code: "LOAD_FAILED", Message: "load failed"}
		return
	}
	s.modelState = StateReady
	s.resident = boolPtr(true)
	s.healthHeld = healthHeld
	s.inferenceReady = true
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
	if s.busyLocked() {
		s.mu.Unlock()
		writeControlError(w, http.StatusConflict, "model is busy", "BUSY")
		return
	}
	if s.modelState == StateUnloaded && !s.inferenceReady {
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
	s.inferenceReady = false
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
		s.inferenceReady = true
		s.resident = boolPtr(true)
		s.lastError = &LastError{Code: "BUSY", Message: "model is busy"}
		return
	}
	if fail {
		s.modelState = StateFailed
		s.inferenceReady = true
		s.resident = boolPtr(true)
		s.lastError = &LastError{Code: "UNLOAD_FAILED", Message: "unload failed"}
		return
	}
	s.modelState = StateUnloaded
	s.inferenceReady = false
	s.resident = boolPtr(false)
	if s.active == nil {
		s.active = intPtr(0)
	} else {
		*s.active = 0
	}
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
			writeControlError(w, http.StatusConflict, "model is busy", "BUSY")
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
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	if !s.inferenceReady {
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
			go s.finishInference(nil)
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

func (s *Server) ReleaseHealth() {
	s.mu.Lock()
	s.healthHeld = false
	s.mu.Unlock()
	if s.healthGate != nil {
		s.healthGate.Release()
	}
}
