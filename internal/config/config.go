package config

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen             = ":8080"
	defaultLogLevel           = "info"
	defaultHealthCheckTimeout = 120
	defaultUnloadTimeout      = 30
	defaultQueueTimeout       = 180
	defaultPrepareTimeout     = 60
	defaultStatusTimeout      = 5
	defaultDrainTimeout       = 180
	defaultShutdownTimeout    = 60
	defaultMaxQueueSize       = 256
	defaultConcurrencyLimit   = 1
	defaultMaxObservationAge  = 5
)

var controlPaths = map[string]struct{}{
	"/control/status": {},
	"/control/load":   {},
	"/control/unload": {},
}

var forbiddenTopLevel = []string{
	"cmd", "cmdStop", "env", "proxy", "checkEndpoint", "groups", "macros",
}

var forbiddenModel = []string{
	"cmd", "cmdStop", "env", "proxy", "checkEndpoint", "groups", "macros",
}

var allowedModelTimeouts = map[string]struct{}{
	"prepareTimeout":     {},
	"healthCheckTimeout": {},
	"unloadTimeout":      {},
}

type rawFile struct {
	Listen             *string             `yaml:"listen"`
	LogLevel           *string             `yaml:"logLevel"`
	HealthCheckTimeout *int                `yaml:"healthCheckTimeout"`
	UnloadTimeout      *int                `yaml:"unloadTimeout"`
	QueueTimeout       *int                `yaml:"queueTimeout"`
	PrepareTimeout     *int                `yaml:"prepareTimeout"`
	StatusTimeout      *int                `yaml:"statusTimeout"`
	DrainTimeout       *int                `yaml:"drainTimeout"`
	ShutdownTimeout    *int                `yaml:"shutdownTimeout"`
	MaxQueueSize       *int                `yaml:"maxQueueSize"`
	Preload            []string            `yaml:"preload"`
	GPU                *rawGPU             `yaml:"gpu"`
	Models             map[string]rawModel `yaml:"models"`
}

type rawGPU struct {
	Device            *string `yaml:"device"`
	SafetyMarginBytes *int64  `yaml:"safetyMarginBytes"`
	MaxObservationAge *int    `yaml:"maxObservationAge"`
}

type rawModel struct {
	BaseURL          *string        `yaml:"baseURL"`
	Name             string         `yaml:"name"`
	Aliases          []string       `yaml:"aliases"`
	ConcurrencyLimit *int           `yaml:"concurrencyLimit"`
	Unlisted         *bool          `yaml:"unlisted"`
	Prepare          *rawPrepare    `yaml:"prepare"`
	Timeouts         map[string]int `yaml:"timeouts"`
	ResourceProfile  *rawProfile    `yaml:"resourceProfile"`
	InferencePaths   []string       `yaml:"inferencePaths"`
	MaxBodyBytes     *int64         `yaml:"maxBodyBytes"`
}

type rawProfile struct {
	ProfileID             *string        `yaml:"profileId"`
	LoadPeakBytes         *int64         `yaml:"loadPeakBytes"`
	InferencePeakBytes    *int64         `yaml:"inferencePeakBytes"`
	UnloadedResidualBytes *int64         `yaml:"unloadedResidualBytes"`
	MaxConcurrency        *int           `yaml:"maxConcurrency"`
	Limits                map[string]any `yaml:"limits"`
}

type rawPrepare struct {
	Argv []string `yaml:"argv"`
}

type Config struct {
	Listen             string
	LogLevel           string
	HealthCheckTimeout time.Duration
	UnloadTimeout      time.Duration
	QueueTimeout       time.Duration
	PrepareTimeout     time.Duration
	StatusTimeout      time.Duration
	DrainTimeout       time.Duration
	ShutdownTimeout    time.Duration
	MaxQueueSize       int
	Preload            []string
	GPU                GPU
	Models             map[string]Model
	aliases            map[string]string
}

type GPU struct {
	Device            string
	SafetyMarginBytes int64
	MaxObservationAge time.Duration
}

type Model struct {
	ID                 string
	BaseURL            string
	Name               string
	Aliases            []string
	ConcurrencyLimit   int
	Unlisted           bool
	Prepare            *Prepare
	PrepareTimeout     time.Duration
	HealthCheckTimeout time.Duration
	UnloadTimeout      time.Duration
	ResourceProfile    ResourceProfile
	InferencePaths     []string
	MaxBodyBytes       int64
}

type ResourceProfile struct {
	ProfileID             string
	LoadPeakBytes         int64
	InferencePeakBytes    int64
	UnloadedResidualBytes int64
	MaxConcurrency        int
	Limits                map[string]any
}

type Prepare struct {
	Argv []string
}

func LoadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Load(data)
}

func Load(data []byte) (*Config, error) {
	var top map[string]any
	if err := yaml.Unmarshal(data, &top); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	if err := rejectKeys(top, forbiddenTopLevel, ""); err != nil {
		return nil, err
	}
	if models, ok := top["models"].(map[string]any); ok {
		for id, raw := range models {
			m, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("config: model %q must be a mapping", id)
			}
			if err := rejectKeys(m, forbiddenModel, "model "+id); err != nil {
				return nil, err
			}
			if timeouts, exists := m["timeouts"]; exists {
				tm, ok := timeouts.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("config: model %q timeouts must be a mapping", id)
				}
				for k := range tm {
					if _, ok := allowedModelTimeouts[k]; !ok {
						return nil, fmt.Errorf("config: model %q timeouts.%s is not an allowed override", id, k)
					}
				}
			}
		}
	}

	var raw rawFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	return raw.toConfig()
}

func rejectKeys(m map[string]any, forbidden []string, where string) error {
	for _, k := range forbidden {
		if _, ok := m[k]; ok {
			if where == "" {
				return fmt.Errorf("config: %s is not supported in v0", k)
			}
			return fmt.Errorf("config: %s: %s is not supported in v0", where, k)
		}
	}
	return nil
}

func (raw rawFile) toConfig() (*Config, error) {
	cfg := &Config{
		Listen:       pickString(raw.Listen, defaultListen),
		LogLevel:     pickString(raw.LogLevel, defaultLogLevel),
		MaxQueueSize: pickInt(raw.MaxQueueSize, defaultMaxQueueSize),
		Preload:      append([]string{}, raw.Preload...),
		Models:       make(map[string]Model),
		aliases:      make(map[string]string),
	}
	var err error
	if cfg.HealthCheckTimeout, err = seconds("healthCheckTimeout", raw.HealthCheckTimeout, defaultHealthCheckTimeout); err != nil {
		return nil, err
	}
	if cfg.UnloadTimeout, err = seconds("unloadTimeout", raw.UnloadTimeout, defaultUnloadTimeout); err != nil {
		return nil, err
	}
	if cfg.QueueTimeout, err = seconds("queueTimeout", raw.QueueTimeout, defaultQueueTimeout); err != nil {
		return nil, err
	}
	if cfg.PrepareTimeout, err = seconds("prepareTimeout", raw.PrepareTimeout, defaultPrepareTimeout); err != nil {
		return nil, err
	}
	if cfg.StatusTimeout, err = seconds("statusTimeout", raw.StatusTimeout, defaultStatusTimeout); err != nil {
		return nil, err
	}
	if cfg.DrainTimeout, err = seconds("drainTimeout", raw.DrainTimeout, defaultDrainTimeout); err != nil {
		return nil, err
	}
	if cfg.ShutdownTimeout, err = seconds("shutdownTimeout", raw.ShutdownTimeout, defaultShutdownTimeout); err != nil {
		return nil, err
	}
	if cfg.MaxQueueSize <= 0 {
		return nil, fmt.Errorf("config: maxQueueSize must be positive")
	}
	if len(raw.Models) == 0 {
		return nil, fmt.Errorf("config: at least one model is required")
	}

	for id, rm := range raw.Models {
		if strings.TrimSpace(id) == "" {
			return nil, fmt.Errorf("config: empty model id")
		}
		m, err := rm.toModel(id, cfg)
		if err != nil {
			return nil, err
		}
		cfg.Models[id] = m
		if _, ok := cfg.aliases[id]; ok {
			return nil, fmt.Errorf("config: model id %q collides with an alias", id)
		}
		cfg.aliases[id] = id
		for _, alias := range m.Aliases {
			if other, ok := cfg.aliases[alias]; ok {
				return nil, fmt.Errorf("config: alias %q for %q collides with %q", alias, id, other)
			}
			cfg.aliases[alias] = id
		}
	}

	for _, id := range cfg.Preload {
		if _, ok := cfg.Models[id]; !ok {
			return nil, fmt.Errorf("config: preload %q is not a registered model", id)
		}
	}
	gpu, err := raw.GPU.toGPU()
	if err != nil {
		return nil, err
	}
	cfg.GPU = gpu
	for id, m := range cfg.Models {
		rm := raw.Models[id]
		profile, paths, body, err := rm.resource(id, m.ConcurrencyLimit)
		if err != nil {
			return nil, err
		}
		m.ResourceProfile = profile
		m.InferencePaths = paths
		m.MaxBodyBytes = body
		cfg.Models[id] = m
	}
	return cfg, nil
}

func (raw *rawGPU) toGPU() (GPU, error) {
	if raw == nil || raw.Device == nil || strings.TrimSpace(*raw.Device) == "" {
		return GPU{}, fmt.Errorf("config: gpu.device is required")
	}
	margin := int64(0)
	if raw.SafetyMarginBytes != nil {
		margin = *raw.SafetyMarginBytes
	}
	if margin < 0 {
		return GPU{}, fmt.Errorf("config: gpu.safetyMarginBytes must not be negative")
	}
	age, err := seconds("gpu.maxObservationAge", raw.MaxObservationAge, defaultMaxObservationAge)
	if err != nil {
		return GPU{}, err
	}
	return GPU{Device: strings.TrimSpace(*raw.Device), SafetyMarginBytes: margin, MaxObservationAge: age}, nil
}

func (rm rawModel) resource(id string, concurrency int) (ResourceProfile, []string, int64, error) {
	if rm.ResourceProfile == nil {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: resourceProfile is required", id)
	}
	p := rm.ResourceProfile
	if p.ProfileID == nil || strings.TrimSpace(*p.ProfileID) == "" {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: resourceProfile.profileId is required", id)
	}
	load, err := nonNeg(id, "loadPeakBytes", p.LoadPeakBytes)
	if err != nil {
		return ResourceProfile{}, nil, 0, err
	}
	inf, err := nonNeg(id, "inferencePeakBytes", p.InferencePeakBytes)
	if err != nil {
		return ResourceProfile{}, nil, 0, err
	}
	residual, err := nonNeg(id, "unloadedResidualBytes", p.UnloadedResidualBytes)
	if err != nil {
		return ResourceProfile{}, nil, 0, err
	}
	peak := load
	if inf > peak {
		peak = inf
	}
	if residual > peak {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: unloadedResidualBytes exceeds peak", id)
	}
	if p.MaxConcurrency == nil || *p.MaxConcurrency <= 0 {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: resourceProfile.maxConcurrency must be positive", id)
	}
	if concurrency > *p.MaxConcurrency {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: concurrencyLimit exceeds maxConcurrency", id)
	}
	if len(rm.InferencePaths) == 0 {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: inferencePaths is required", id)
	}
	paths := make([]string, 0, len(rm.InferencePaths))
	seen := map[string]struct{}{}
	for _, path := range rm.InferencePaths {
		if !strings.HasPrefix(path, "/") || strings.Contains(path, "?") {
			return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: inference path %q is invalid", id, path)
		}
		if _, ok := controlPaths[path]; ok {
			return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: inference path %q is a control path", id, path)
		}
		if _, ok := seen[path]; ok {
			return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: duplicate inference path %q", id, path)
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}
	if rm.MaxBodyBytes == nil {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: maxBodyBytes is required", id)
	}
	if *rm.MaxBodyBytes < 0 {
		return ResourceProfile{}, nil, 0, fmt.Errorf("config: model %q: maxBodyBytes must not be negative", id)
	}
	limits := map[string]any{}
	for k, v := range p.Limits {
		limits[k] = v
	}
	return ResourceProfile{
		ProfileID:             strings.TrimSpace(*p.ProfileID),
		LoadPeakBytes:         load,
		InferencePeakBytes:    inf,
		UnloadedResidualBytes: residual,
		MaxConcurrency:        *p.MaxConcurrency,
		Limits:                limits,
	}, paths, *rm.MaxBodyBytes, nil
}

func nonNeg(id, name string, v *int64) (int64, error) {
	if v == nil {
		return 0, fmt.Errorf("config: model %q: resourceProfile.%s is required", id, name)
	}
	if *v < 0 {
		return 0, fmt.Errorf("config: model %q: resourceProfile.%s must not be negative", id, name)
	}
	return *v, nil
}

func (rm rawModel) toModel(id string, globals *Config) (Model, error) {
	if rm.BaseURL == nil || strings.TrimSpace(*rm.BaseURL) == "" {
		return Model{}, fmt.Errorf("config: model %q: baseURL is required", id)
	}
	u, err := url.Parse(*rm.BaseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Model{}, fmt.Errorf("config: model %q: baseURL is invalid", id)
	}
	limit := defaultConcurrencyLimit
	if rm.ConcurrencyLimit != nil {
		limit = *rm.ConcurrencyLimit
	}
	if limit <= 0 {
		return Model{}, fmt.Errorf("config: model %q: concurrencyLimit must be positive", id)
	}
	unlisted := false
	if rm.Unlisted != nil {
		unlisted = *rm.Unlisted
	}
	m := Model{
		ID:                 id,
		BaseURL:            strings.TrimRight(*rm.BaseURL, "/"),
		Name:               rm.Name,
		Aliases:            append([]string{}, rm.Aliases...),
		ConcurrencyLimit:   limit,
		Unlisted:           unlisted,
		PrepareTimeout:     globals.PrepareTimeout,
		HealthCheckTimeout: globals.HealthCheckTimeout,
		UnloadTimeout:      globals.UnloadTimeout,
	}
	if m.Name == "" {
		m.Name = id
	}
	if rm.Timeouts != nil {
		if v, ok := rm.Timeouts["prepareTimeout"]; ok {
			if m.PrepareTimeout, err = seconds("models."+id+".timeouts.prepareTimeout", &v, 0); err != nil {
				return Model{}, err
			}
		}
		if v, ok := rm.Timeouts["healthCheckTimeout"]; ok {
			if m.HealthCheckTimeout, err = seconds("models."+id+".timeouts.healthCheckTimeout", &v, 0); err != nil {
				return Model{}, err
			}
		}
		if v, ok := rm.Timeouts["unloadTimeout"]; ok {
			if m.UnloadTimeout, err = seconds("models."+id+".timeouts.unloadTimeout", &v, 0); err != nil {
				return Model{}, err
			}
		}
	}
	if rm.Prepare != nil {
		if len(rm.Prepare.Argv) == 0 {
			return Model{}, fmt.Errorf("config: model %q: prepare.argv must not be empty", id)
		}
		if !filepath.IsAbs(rm.Prepare.Argv[0]) {
			return Model{}, fmt.Errorf("config: model %q: prepare.argv[0] must be an absolute path", id)
		}
		m.Prepare = &Prepare{Argv: append([]string{}, rm.Prepare.Argv...)}
	}
	for _, alias := range m.Aliases {
		if strings.TrimSpace(alias) == "" {
			return Model{}, fmt.Errorf("config: model %q: empty alias", id)
		}
		if alias == id {
			return Model{}, fmt.Errorf("config: model %q: alias collides with canonical id", id)
		}
	}
	return m, nil
}

func (c *Config) Resolve(idOrAlias string) (string, bool) {
	if c.aliases == nil {
		c.Index()
	}
	canonical, ok := c.aliases[idOrAlias]
	return canonical, ok
}

func (c *Config) Index() {
	c.aliases = make(map[string]string)
	for id, m := range c.Models {
		c.aliases[id] = id
		for _, alias := range m.Aliases {
			c.aliases[alias] = id
		}
	}
}

func (m Model) AllowsPath(path string) bool {
	for _, p := range m.InferencePaths {
		if p == path {
			return true
		}
	}
	return false
}

func (m Model) LoadBound() int64 {
	if m.ResourceProfile.InferencePeakBytes > m.ResourceProfile.LoadPeakBytes {
		return m.ResourceProfile.InferencePeakBytes
	}
	return m.ResourceProfile.LoadPeakBytes
}

func (c *Config) Model(idOrAlias string) (Model, bool) {
	canonical, ok := c.Resolve(idOrAlias)
	if !ok {
		return Model{}, false
	}
	m, ok := c.Models[canonical]
	return m, ok
}

func pickString(v *string, fallback string) string {
	if v == nil || *v == "" {
		return fallback
	}
	return *v
}

func pickInt(v *int, fallback int) int {
	if v == nil {
		return fallback
	}
	return *v
}

func seconds(name string, v *int, fallback int) (time.Duration, error) {
	n := fallback
	if v != nil {
		n = *v
	}
	if n <= 0 {
		return 0, fmt.Errorf("config: %s must be positive", name)
	}
	return time.Duration(n) * time.Second, nil
}
