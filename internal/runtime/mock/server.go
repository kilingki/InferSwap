package mock

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/kilingki/InferSwap/internal/testkit"
)

// Server is an in-process mock of a model-project control+inference endpoint.
// A and B must be separate instances; each owns its own port, counters, and state.
type Server struct {
	name string

	mu      sync.Mutex
	started bool
	closed  bool

	publicLn net.Listener
	public   *http.Server
	admin    *http.Server
	adminLn  net.Listener

	endpointAlive bool
	prepareFail   bool
	loadFail      LoadFailMode
	unloadFail    bool
	persistInfer  bool
	statusMode    StatusMode

	prepareGate *testkit.Barrier
	loadGate    *testkit.Barrier
	unloadGate  *testkit.Barrier
	inferGate   *testkit.Barrier
	backendGate *testkit.Barrier

	modelState WireState
	residency  Residency
	active     int
	lastError  *LastError
	counts     Counts

	loadCh   chan struct{}
	unloadCh chan struct{}
	lastPath string
}

type Option func(*Server)

func WithEndpointDown() Option {
	return func(s *Server) { s.endpointAlive = false }
}

func WithPrepareGate(g *testkit.Barrier) Option { return func(s *Server) { s.prepareGate = g } }
func WithLoadGate(g *testkit.Barrier) Option    { return func(s *Server) { s.loadGate = g } }
func WithUnloadGate(g *testkit.Barrier) Option  { return func(s *Server) { s.unloadGate = g } }
func WithInferGate(g *testkit.Barrier) Option   { return func(s *Server) { s.inferGate = g } }
func WithBackendGate(g *testkit.Barrier) Option { return func(s *Server) { s.backendGate = g } }

func WithPrepareFailure() Option { return func(s *Server) { s.prepareFail = true } }
func WithLoadFailure() Option    { return func(s *Server) { s.loadFail = LoadFailNotResident } }
func WithLoadFailureUnknown() Option {
	return func(s *Server) { s.loadFail = LoadFailUnknown }
}
func WithUnloadFailure() Option { return func(s *Server) { s.unloadFail = true } }
func WithPersistInference() Option {
	return func(s *Server) { s.persistInfer = true }
}
func WithInitialLoading() Option {
	return func(s *Server) {
		s.modelState = StateLoading
		s.residency = ResidencyUnknown
	}
}

func New(name string, opts ...Option) (*Server, error) {
	s := &Server{
		name:          name,
		endpointAlive: true,
		modelState:    StateUnloaded,
		residency:     ResidencyNotResident,
		active:        0,
	}
	for _, opt := range opts {
		opt(s)
	}

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/control/load", s.handleLoad)
	publicMux.HandleFunc("/control/unload", s.handleUnload)
	publicMux.HandleFunc("/control/status", s.handleStatus)
	publicMux.HandleFunc("/v1/", s.handleInference)
	publicMux.HandleFunc("/align", s.handleInference)

	publicLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s.publicLn = publicLn
	s.public = &http.Server{Handler: s.wrapPublic(publicMux), ReadHeaderTimeout: 5 * time.Second}

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/prepare", s.handlePrepare)
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = publicLn.Close()
		return nil, err
	}
	s.adminLn = adminLn
	s.admin = &http.Server{Handler: adminMux, ReadHeaderTimeout: 5 * time.Second}

	go func() { _ = s.public.Serve(publicLn) }()
	go func() { _ = s.admin.Serve(adminLn) }()
	s.started = true
	return s, nil
}

func (s *Server) wrapPublic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		alive := s.endpointAlive
		mode := s.statusMode
		s.mu.Unlock()
		if !alive || mode == StatusTransportError {
			hj, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "endpoint down", http.StatusServiceUnavailable)
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				return
			}
			_ = conn.Close()
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Name() string { return s.name }

func (s *Server) URL() string {
	return "http://" + s.publicLn.Addr().String()
}

func (s *Server) AdminURL() string {
	return "http://" + s.adminLn.Addr().String()
}

func (s *Server) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if s.public != nil {
		_ = s.public.Shutdown(ctx)
	}
	if s.admin != nil {
		_ = s.admin.Shutdown(ctx)
	}
}

func (s *Server) Counts() Counts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts
}

func (s *Server) Snapshot() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statusLocked()
}

func (s *Server) SetStatusMode(mode StatusMode) {
	s.mu.Lock()
	s.statusMode = mode
	s.mu.Unlock()
}

func (s *Server) SetLoadFailure(v bool) {
	s.mu.Lock()
	if v {
		s.loadFail = LoadFailNotResident
	} else {
		s.loadFail = LoadOK
	}
	s.mu.Unlock()
}

func (s *Server) SetLoadFailureMode(mode LoadFailMode) {
	s.mu.Lock()
	s.loadFail = mode
	s.mu.Unlock()
}

func (s *Server) SetUnloadFailure(v bool) {
	s.mu.Lock()
	s.unloadFail = v
	s.mu.Unlock()
}

func (s *Server) SetPrepareFailure(v bool) {
	s.mu.Lock()
	s.prepareFail = v
	s.mu.Unlock()
}

func (s *Server) SetEndpointAlive(v bool) {
	s.mu.Lock()
	s.endpointAlive = v
	s.mu.Unlock()
}

func (s *Server) LastPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastPath
}

func (s *Server) EndpointAlive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endpointAlive
}

func (s *Server) ForceReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelState = StateReady
	s.residency = ResidencyResident
	s.active = 0
	s.lastError = nil
}

func (s *Server) ForceUnloaded() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelState = StateUnloaded
	s.residency = ResidencyNotResident
	s.active = 0
	s.lastError = nil
}

func (s *Server) ForceFailed(res Residency) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelState = StateFailed
	s.residency = res
	s.active = 0
	s.lastError = &LastError{Code: "LOAD_FAILED", Message: "injected"}
}

func (s *Server) Prepare(ctx context.Context) error {
	s.mu.Lock()
	s.counts.PrepareStarts++
	fail := s.prepareFail
	gate := s.prepareGate
	already := s.endpointAlive
	s.mu.Unlock()

	if !already && gate != nil {
		if err := gate.Hit(ctx); err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts.PrepareEnds++
	if fail {
		return errors.New("prepare failed")
	}
	if already {
		return nil
	}
	s.endpointAlive = true
	s.modelState = StateUnloaded
	s.residency = ResidencyNotResident
	s.active = 0
	s.lastError = nil
	return nil
}

func (s *Server) handlePrepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeControlError(w, http.StatusMethodNotAllowed, "POST required", "BAD_REQUEST")
		return
	}
	if err := s.Prepare(r.Context()); err != nil {
		writeControlError(w, http.StatusInternalServerError, err.Error(), "PREPARE_FAILED")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) statusLocked() Status {
	return Status{
		State:          s.modelState,
		Residency:      s.residency,
		ActiveRequests: s.active,
		LastError:      s.lastError,
	}
}

func (s *Server) busyLocked() bool {
	return s.active > 0
}

func (s *Server) addActiveLocked(delta int) {
	s.active += delta
	if s.active < 0 {
		s.active = 0
	}
}
