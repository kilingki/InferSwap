package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kilingki/InferSwap/internal/config"
	"github.com/kilingki/InferSwap/internal/router"
	"github.com/kilingki/InferSwap/internal/runtime"
	"github.com/kilingki/InferSwap/internal/server"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to InferSwap config YAML")
	flag.Parse()

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	level, err := config.ParseLogLevel(cfg.LogLevel)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	runtimes := make(map[string]runtime.Runtime, len(cfg.Models))
	for id, model := range cfg.Models {
		runtimes[id] = runtime.NewClient(nil, model, cfg, runtime.ClientOptions{})
	}
	rt := router.NewExclusive(cfg, runtimes, nil)
	bootCtx, bootCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout+cfg.HealthCheckTimeout+time.Minute)
	defer bootCancel()
	if err := rt.Reconcile(bootCtx); err != nil {
		log.Fatalf("reconcile: %v", err)
	}
	if err := rt.Preload(bootCtx, cfg.Preload); err != nil {
		log.Fatalf("preload: %v", err)
	}

	srv := server.New(cfg, rt, runtimes)
	httpSrv := &http.Server{Addr: cfg.Listen, Handler: srv.Handler()}
	go func() {
		slog.Info("inferswap listening", "addr", cfg.Listen)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	if err := rt.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown", "err", err, "remaining", rt.Remaining())
		os.Exit(1)
	}
}
