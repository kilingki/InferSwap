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
	"sync/atomic"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/testkit"
)

type readyReq struct {
	respond chan error
}

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

func (c *Client) EnsureReady(ctx context.Context, _ time.Duration) error {
	req := readyReq{respond: make(chan error, 1)}
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

	startOp := func() {
		setState(StateStarting)
		gen := c.gen.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		opCancel = cancel
		opDone = make(chan opResult, 1)
		go func() {
			opDone <- opResult{gen: gen, err: c.doEnsure(ctx)}
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
				startOp()
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

func (c *Client) doEnsure(ctx context.Context) error {
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
	if st.ProtocolVersion != "" && st.ProtocolVersion != "v0" {
		return fmt.Errorf("unsupported protocol_version %q", st.ProtocolVersion)
	}

	switch st.ModelState {
	case "ready":
		if err := c.waitHealth(ctx, c.clock.Now().Add(c.healthCheckTimeout)); err != nil {
			return err
		}
		return nil
	case "loading":
		return c.waitHealth(ctx, c.clock.Now().Add(c.healthCheckTimeout))
	case "unloading":
		return fmt.Errorf("runtime is unloading")
	case "failed", "unknown":
		if st.ModelState == "failed" || st.ModelState == "unknown" {
			return fmt.Errorf("remote model_state=%s; refusing to assume ready or stopped", st.ModelState)
		}
	}

	if err := c.postControl(ctx, "/control/load", c.healthCheckTimeout); err != nil {
		return err
	}
	return c.waitHealth(ctx, c.clock.Now().Add(c.healthCheckTimeout))
}

func (c *Client) runPrepare(ctx context.Context) error {
	if c.prepare == nil || len(c.prepare.Argv) == 0 {
		return ErrPrepareUnavailable
	}
	cwd := prepareCwd(c.prepare.Argv)
	done := make(chan error, 1)
	go func() {
		done <- c.commander.Run(ctx, c.prepare.Argv, cwd)
	}()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("prepare failed: %w", err)
		}
		return nil
	case <-c.clock.After(c.prepareTimeout):
		return fmt.Errorf("prepare timed out after %s", c.prepareTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) doStop(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.postControl(ctx, "/control/unload", timeout); err != nil {
		return err
	}
	st, err := c.fetchStatus(ctx)
	if err != nil {
		return fmt.Errorf("unload succeeded but status recheck failed: %w", err)
	}
	if st.InferenceReady || (st.Resident != nil && *st.Resident) || (st.ActiveRequests != nil && *st.ActiveRequests != 0) {
		return fmt.Errorf("unload did not reach idle unloaded state")
	}
	return nil
}

func (c *Client) postControl(ctx context.Context, path string, timeout time.Duration) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewBufferString(`{}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, bytes.TrimSpace(body))
	}
	return nil
}

func (c *Client) waitHealth(ctx context.Context, deadline time.Time) error {
	first := true
	for {
		if !c.clock.Now().Before(deadline) {
			return fmt.Errorf("health check timed out after %s", c.healthCheckTimeout)
		}
		reqCtx, cancel := context.WithTimeout(ctx, c.statusTimeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, c.baseURL+"/health", nil)
		if err != nil {
			cancel()
			return err
		}
		resp, err := c.http.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			cancel()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		cancel()
		if first {
			first = false
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
	if isEndpointDown(err) {
		return nil
	}
	if err != nil {
		return nil
	}
	switch st.ModelState {
	case "ready":
		c.installProxy()
		setState(StateReady)
	case "unloaded":
		setState(StateStopped)
	case "loading":
		setState(StateStarting)
	case "unloading":
		setState(StateStopping)
	case "failed":
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
	if !json.Valid(body) {
		return Status{}, fmt.Errorf("status: malformed json")
	}
	var st Status
	if err := json.Unmarshal(body, &st); err != nil {
		return Status{}, fmt.Errorf("status: %w", err)
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
