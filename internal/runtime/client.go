package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/testkit"
)

type readyReq struct {
	respond chan error
	timeout time.Duration
}

var errAdmissionRequired = errors.New("load refused without admission")

type stopReq struct {
	timeout time.Duration
	respond chan error
}

type opResult struct {
	gen uint64
	err error
}

type reconReq struct {
	ctx     context.Context
	respond chan error
}

type Client struct {
	id                 string
	baseURL            string
	prepare            *config.Prepare
	prepareTimeout     time.Duration
	healthCheckTimeout time.Duration
	unloadTimeout      time.Duration
	statusTimeout      time.Duration
	clock              testkit.Clock
	http               *http.Client
	commander          Commander

	parent context.Context
	cancel context.CancelFunc

	readyCh chan readyReq
	stopCh  chan stopReq
	reconCh chan reconReq

	state   atomic.Value
	handler atomic.Pointer[http.Handler]
	gen     atomic.Uint64

	prepMu   sync.Mutex
	prepBusy bool
	loadGate func(context.Context) error
}

type ClientOptions struct {
	Clock     testkit.Clock
	HTTP      *http.Client
	Commander Commander
}

func NewClient(parent context.Context, model config.Model, globals *config.Config, opts ClientOptions) *Client {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	clock := opts.Clock
	if clock == nil {
		clock = testkit.RealClock{}
	}
	httpClient := opts.HTTP
	if httpClient == nil {
		httpClient = &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}}
	}
	cmd := opts.Commander
	if cmd == nil {
		cmd = ExecCommander{}
	}
	statusTO := 5 * time.Second
	if globals != nil && globals.StatusTimeout > 0 {
		statusTO = globals.StatusTimeout
	}
	c := &Client{
		id:                 model.ID,
		baseURL:            strings.TrimRight(model.BaseURL, "/"),
		prepare:            model.Prepare,
		prepareTimeout:     model.PrepareTimeout,
		healthCheckTimeout: model.HealthCheckTimeout,
		unloadTimeout:      model.UnloadTimeout,
		statusTimeout:      statusTO,
		clock:              clock,
		http:               httpClient,
		commander:          cmd,
		parent:             ctx,
		cancel:             cancel,
		readyCh:            make(chan readyReq),
		stopCh:             make(chan stopReq),
		reconCh:            make(chan reconReq),
	}
	c.state.Store(StateUnknown)
	go c.run()
	return c
}

func (c *Client) ID() string { return c.id }

func (c *Client) SetLoadGate(fn func(context.Context) error) {
	c.prepMu.Lock()
	c.loadGate = fn
	c.prepMu.Unlock()
}

func (c *Client) State() State {
	if s, ok := c.state.Load().(State); ok {
		return s
	}
	return StateUnknown
}

func (c *Client) Shutdown() {
	c.cancel()
}

func (c *Client) Reconcile(ctx context.Context) error {
	req := reconReq{ctx: ctx, respond: make(chan error, 1)}
	select {
	case c.reconCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.parent.Done():
		return ErrShutdown
	}
	select {
	case err := <-req.respond:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.parent.Done():
		return ErrShutdown
	}
}

func (c *Client) EnsureReady(ctx context.Context, timeout time.Duration) error {
	req := readyReq{respond: make(chan error, 1), timeout: timeout}
	select {
	case c.readyCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.parent.Done():
		return ErrShutdown
	}
	select {
	case err := <-req.respond:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.parent.Done():
		return ErrShutdown
	}
}

func (c *Client) Stop(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = c.unloadTimeout
	}
	req := stopReq{timeout: timeout, respond: make(chan error, 1)}
	select {
	case c.stopCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.parent.Done():
		return ErrShutdown
	}
	select {
	case err := <-req.respond:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.parent.Done():
		return ErrShutdown
	}
}

func (c *Client) Status(ctx context.Context) (Status, error) {
	return c.fetchStatus(ctx)
}

func (c *Client) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := c.handler.Load()
	if h == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "runtime is not ready",
			"src":   "inferswap",
			"code":  "NOT_READY",
		})
		return
	}
	(*h).ServeHTTP(w, r)
}

func (c *Client) run() {
	state := StateUnknown
	setState := func(s State) {
		state = s
		c.state.Store(s)
	}
	var waiters []readyReq
	var opCancel context.CancelFunc
	var opDone chan opResult

	notify := func(err error) {
		for _, w := range waiters {
			select {
			case w.respond <- err:
			default:
			}
		}
		waiters = nil
	}

	startOp := func(timeout time.Duration) {
		setState(StateStarting)
		gen := c.gen.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		opCancel = cancel
		opDone = make(chan opResult, 1)
		go func() {
			opDone <- opResult{gen: gen, err: c.doEnsure(ctx, timeout)}
		}()
	}

	for {
		select {
		case <-c.parent.Done():
			setState(StateShutdown)
			if opCancel != nil {
				opCancel()
			}
			c.handler.Store(nil)
			notify(ErrShutdown)
			return

		case req := <-c.reconCh:
			if state == StateShutdown {
				req.respond <- ErrShutdown
				continue
			}
			req.respond <- c.applyReconcile(req.ctx, setState)
			if state == StateStarting && opDone == nil {
				startOp(0)
			}

		case req := <-c.readyCh:
			switch state {
			case StateShutdown:
				req.respond <- ErrShutdown
			case StateReady:
				req.respond <- nil
			case StateStarting:
				waiters = append(waiters, req)
			default:
				waiters = append(waiters, req)
				startOp(req.timeout)
			}

		case res := <-opDone:
			if opCancel != nil {
				opCancel()
			}
			opCancel = nil
			opDone = nil
			if res.gen != c.gen.Load() {
				continue
			}
			if res.err != nil {
				c.handler.Store(nil)
				setState(StateFailed)
				notify(res.err)
				continue
			}
			c.installProxy()
			setState(StateReady)
			notify(nil)

		case req := <-c.stopCh:
			if state == StateShutdown {
				req.respond <- ErrShutdown
				continue
			}
			if opDone != nil {
				c.gen.Add(1)
				if opCancel != nil {
					opCancel()
				}
				<-opDone
				opCancel = nil
				opDone = nil
			}
			setState(StateStopping)
			err := c.doStop(req.timeout)
			c.handler.Store(nil)
			if err != nil {
				setState(StateFailed)
			} else {
				setState(StateStopped)
			}
			notify(err)
			req.respond <- err
		}
	}
}

func (c *Client) doEnsure(ctx context.Context, timeout time.Duration) error {
	st, err := c.fetchStatus(ctx)
	if isEndpointDown(err) {
		if err := c.runPrepare(ctx); err != nil {
			return err
		}
		st, err = c.fetchStatus(ctx)
	}
	if err != nil {
		if isEndpointDown(err) {
			return fmt.Errorf("endpoint still unavailable after prepare: %w", err)
		}
		return err
	}
	return c.ensureFromStatus(ctx, st, timeout)
}

func (c *Client) loadBudget(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	return c.healthCheckTimeout
}

func (c *Client) ensureFromStatus(ctx context.Context, st Status, timeout time.Duration) error {
	budget := c.loadBudget(timeout)
	switch st.State {
	case WireReady:
		return nil
	case WireLoading:
		return c.waitReady(ctx, c.clock.Now().Add(budget))
	case WireUnloading:
		return fmt.Errorf("runtime is unloading")
	case WireFailed:
		if st.Residency == ResidencyNotResident && st.ActiveRequests == 0 {
			break
		}
		return fmt.Errorf("remote state=failed residency=%s; refusing load", st.Residency)
	case WireUnloaded:
	default:
		return fmt.Errorf("remote state=%s; refusing to assume ready or stopped", st.State)
	}
	if err := c.waitLoadGate(ctx); err != nil {
		return err
	}
	deadline := c.clock.Now().Add(budget)
	st, err := c.postLoad(ctx, deadline)
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err) {
			return err
		}
		observed, ferr := c.fetchStatus(ctx)
		if ferr != nil {
			return err
		}
		if observed.State == WireReady {
			return nil
		}
		if observed.State == WireLoading {
			return c.waitReady(ctx, deadline)
		}
		if observed.State == WireFailed && observed.Residency == ResidencyNotResident && observed.ActiveRequests == 0 {
			return fmt.Errorf("load failed: %s", lastCode(observed))
		}
		return fmt.Errorf("load not confirmed: state=%s residency=%s", observed.State, observed.Residency)
	}
	if st.State != WireReady || st.Residency != ResidencyResident {
		return fmt.Errorf("load response is not ready/resident")
	}
	return nil
}

func (c *Client) runPrepare(ctx context.Context) error {
	c.prepMu.Lock()
	if c.prepBusy {
		c.prepMu.Unlock()
		return fmt.Errorf("prepare still running")
	}
	if c.prepare == nil || len(c.prepare.Argv) == 0 {
		c.prepMu.Unlock()
		return ErrPrepareUnavailable
	}
	c.prepBusy = true
	argv := append([]string{}, c.prepare.Argv...)
	c.prepMu.Unlock()

	cwd := prepareCwd(argv)
	done := make(chan error, 1)
	go func() {
		done <- c.commander.Run(c.parent, argv, cwd)
	}()
	finish := func() {
		c.prepMu.Lock()
		c.prepBusy = false
		c.prepMu.Unlock()
	}
	select {
	case err := <-done:
		finish()
		if err != nil {
			return fmt.Errorf("prepare failed: %w", err)
		}
		return nil
	case <-c.clock.After(c.prepareTimeout):
		go func() {
			<-done
			finish()
		}()
		return fmt.Errorf("prepare timed out after %s", c.prepareTimeout)
	case <-ctx.Done():
		go func() {
			<-done
			finish()
		}()
		return ctx.Err()
	}
}

func (c *Client) doStop(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	st, err := c.fetchStatus(ctx)
	if err != nil {
		return err
	}
	if st.State == WireUnloaded && st.Residency == ResidencyNotResident && st.ActiveRequests == 0 {
		return nil
	}
	if st.ActiveRequests > 0 {
		return fmt.Errorf("unload refused while active_requests=%d", st.ActiveRequests)
	}
	if st.State == WireLoading {
		return fmt.Errorf("refusing unload while loading")
	}
	body, err := c.postControl(ctx, "/control/unload", timeout)
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err) {
			return err
		}
		observed, ferr := c.fetchStatus(context.Background())
		if ferr != nil {
			return err
		}
		if observed.State == WireUnloaded && observed.Residency == ResidencyNotResident && observed.ActiveRequests == 0 {
			return nil
		}
		return fmt.Errorf("unload not confirmed: state=%s residency=%s", observed.State, observed.Residency)
	}
	parsed, err := ParseStatus(body)
	if err != nil {
		return err
	}
	if parsed.State != WireUnloaded || parsed.Residency != ResidencyNotResident || parsed.ActiveRequests != 0 {
		return fmt.Errorf("unload response is not unloaded")
	}
	return nil
}

func (c *Client) waitLoadGate(ctx context.Context) error {
	c.prepMu.Lock()
	fn := c.loadGate
	c.prepMu.Unlock()
	if fn == nil {
		return errAdmissionRequired
	}
	return fn(ctx)
}

func (c *Client) postLoad(ctx context.Context, deadline time.Time) (Status, error) {
	remaining := deadline.Sub(c.clock.Now())
	if remaining <= 0 {
		return Status{}, fmt.Errorf("load deadline exceeded")
	}
	body, err := c.postControl(ctx, "/control/load", remaining)
	if err != nil {
		return Status{}, err
	}
	st, err := ParseStatus(body)
	if err != nil {
		return Status{}, err
	}
	return st, nil
}

func (c *Client) postControl(ctx context.Context, path string, timeout time.Duration) ([]byte, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewBufferString(`{}`))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		code, message, perr := ParseControlError(body)
		if perr != nil {
			return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, bytes.TrimSpace(body))
		}
		return nil, &ControlFault{Status: resp.StatusCode, Code: code, Message: message}
	}
	return body, nil
}

func (c *Client) waitReady(ctx context.Context, deadline time.Time) error {
	for {
		if !c.clock.Now().Before(deadline) {
			return fmt.Errorf("ready check timed out")
		}
		st, err := c.fetchStatus(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
		} else {
			switch st.State {
			case WireReady:
				return nil
			case WireFailed:
				return fmt.Errorf("remote state=failed residency=%s", st.Residency)
			case WireUnloaded:
				return fmt.Errorf("load ended unloaded")
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.clock.After(time.Second):
		}
	}
}

func (c *Client) applyReconcile(ctx context.Context, setState func(State)) error {
	st, err := c.fetchStatus(ctx)
	if err != nil {
		setState(StateUnknown)
		c.handler.Store(nil)
		return nil
	}
	switch st.State {
	case WireReady:
		c.installProxy()
		setState(StateReady)
	case WireUnloaded:
		c.handler.Store(nil)
		setState(StateStopped)
	case WireLoading:
		setState(StateStarting)
	case WireUnloading:
		setState(StateStopping)
	case WireFailed:
		c.handler.Store(nil)
		setState(StateFailed)
	default:
		setState(StateUnknown)
	}
	return nil
}

func (c *Client) fetchStatus(ctx context.Context) (Status, error) {
	reqCtx, cancel := context.WithTimeout(ctx, c.statusTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, c.baseURL+"/control/status", nil)
	if err != nil {
		return Status{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Status{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Status{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Status{}, fmt.Errorf("status: HTTP %d", resp.StatusCode)
	}
	st, err := ParseStatus(body)
	if err != nil {
		return Status{}, err
	}
	return st, nil
}

func (c *Client) installProxy() {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.ModifyResponse = func(resp *http.Response) error {
		if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
			resp.Header.Set("X-Accel-Buffering", "no")
		}
		return nil
	}
	var h http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if rec != http.ErrAbortHandler {
					panic(rec)
				}
			}
		}()
		proxy.ServeHTTP(w, r)
	})
	c.handler.Store(&h)
}

func isEndpointDown(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return false
		}
		return isEndpointDown(urlErr.Err)
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "server closed") ||
		strings.Contains(strings.ToLower(msg), "eof")
}

func lastCode(st Status) string {
	if st.LastError == nil {
		return ""
	}
	return st.LastError.Code
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Timeout() {
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
