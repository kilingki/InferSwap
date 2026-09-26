package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/proxy"
	"github.com/kilingki/InferSwap/internal/router"
	"github.com/kilingki/InferSwap/internal/runtime"
)

type Server struct {
	cfg    *config.Config
	router *router.Router
	rts    map[string]runtime.Runtime
}

func New(cfg *config.Config, rt *router.Router, runtimes map[string]runtime.Runtime) *Server {
	if cfg != nil {
		cfg.Index()
	}
	return &Server{cfg: cfg, router: rt, rts: runtimes}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /v1/models", s.handleModels)
	mux.HandleFunc("POST /v1/chat/completions", s.handleInference)
	mux.HandleFunc("POST /v1/completions", s.handleInference)
	mux.HandleFunc("POST /v1/embeddings", s.handleInference)
	mux.HandleFunc("POST /v1/audio/transcriptions", s.handleInference)
	mux.HandleFunc("POST /align", s.handleAlign)
	mux.HandleFunc("/", s.handleUnsupported)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleUnsupported(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" || r.URL.Path == "/v1/models" {
		return
	}
	proxy.WriteError(w, &proxy.ExtractError{Status: http.StatusNotFound, Code: "NOT_FOUND", Msg: "unsupported path"})
}

func (s *Server) handleAlign(w http.ResponseWriter, r *http.Request) {
	model := r.URL.Query().Get("model")
	if model == "" {
		proxy.WriteError(w, proxy.ErrNoModel)
		return
	}
	canonical, ok := s.cfg.Resolve(model)
	if !ok {
		proxy.WriteError(w, &proxy.ExtractError{Status: http.StatusNotFound, Code: "NOT_FOUND", Msg: "model not found"})
		return
	}
	m := s.cfg.Models[canonical]
	if !m.AllowsPath("/align") {
		proxy.WriteError(w, &proxy.ExtractError{Status: http.StatusNotFound, Code: "NOT_FOUND", Msg: "unsupported path"})
		return
	}
	if r.ContentLength > m.MaxBodyBytes {
		proxy.WriteError(w, proxy.ErrBodyTooLarge)
		return
	}
	q := r.URL.Query()
	q.Del("model")
	r.URL.RawQuery = q.Encode()
	r.Body = http.MaxBytesReader(w, r.Body, m.MaxBodyBytes)
	s.router.ServeModel(canonical, w, r)
}

func (s *Server) handleInference(w http.ResponseWriter, r *http.Request) {
	model, err := s.extractInferenceModel(r)
	if err != nil {
		proxy.WriteError(w, err)
		return
	}
	canonical, ok := s.cfg.Resolve(model)
	if !ok {
		proxy.WriteError(w, &proxy.ExtractError{Status: http.StatusNotFound, Code: "NOT_FOUND", Msg: "model not found"})
		return
	}
	m := s.cfg.Models[canonical]
	if len(m.InferencePaths) > 0 && !m.AllowsPath(r.URL.Path) {
		proxy.WriteError(w, &proxy.ExtractError{Status: http.StatusNotFound, Code: "NOT_FOUND", Msg: "unsupported path"})
		return
	}
	if m.MaxBodyBytes > 0 && r.ContentLength > m.MaxBodyBytes {
		proxy.WriteError(w, proxy.ErrBodyTooLarge)
		return
	}
	if m.MaxBodyBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, m.MaxBodyBytes)
	}
	s.router.ServeModel(canonical, w, r)
}

func (s *Server) extractInferenceModel(r *http.Request) (string, error) {
	limit := int64(proxy.MaxBodyBytes)
	if q := r.URL.Query().Get("model"); q != "" {
		if canonical, ok := s.cfg.Resolve(q); ok {
			if m := s.cfg.Models[canonical]; m.MaxBodyBytes > 0 {
				limit = m.MaxBodyBytes
			}
		}
		return proxy.ExtractModelLimited(r, limit)
	}
	var max int64
	for _, m := range s.cfg.Models {
		if m.MaxBodyBytes > max {
			max = m.MaxBodyBytes
		}
	}
	if max > 0 {
		limit = max
	}
	return proxy.ExtractModelLimited(r, limit)
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	type rec struct {
		ID      string         `json:"id"`
		Object  string         `json:"object"`
		OwnedBy string         `json:"owned_by"`
		Name    string         `json:"name,omitempty"`
		Status  map[string]any `json:"status"`
	}
	out := make([]rec, 0)
	ids := make([]string, 0, len(s.cfg.Models))
	for id := range s.cfg.Models {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		m := s.cfg.Models[id]
		if m.Unlisted {
			continue
		}
		st := map[string]any{
			"state":     "unknown",
			"ready":     nil,
			"residency": nil,
		}
		if rt, ok := s.rts[id]; ok {
			state := rt.State()
			st["state"] = string(state)
			switch state {
			case runtime.StateUnknown:
				st["ready"] = nil
				st["residency"] = nil
			default:
				st["ready"] = state == runtime.StateReady
				if remote, err := rt.Status(r.Context()); err == nil {
					st["residency"] = string(remote.Residency)
				}
			}
		}
		out = append(out, rec{
			ID:      id,
			Object:  "model",
			OwnedBy: "inferswap",
			Name:    m.Name,
			Status:  st,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": out})
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.router == nil {
		return nil
	}
	return s.router.Shutdown(ctx)
}
