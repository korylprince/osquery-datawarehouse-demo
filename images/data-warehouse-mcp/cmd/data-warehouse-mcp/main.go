package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/config"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/httpapi"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/logging"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/mcpserver"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/sandbox"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/session"
	trinoapp "github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/trino"
)

const version = "0.1.0"

func main() {
	logger := logging.New()
	slog.SetDefault(logger)

	cfg, err := config.Parse()
	if err != nil {
		logger.Error("failed to parse config", "error", err.Error())
		os.Exit(1)
	}

	metrics := httpapi.NewMetrics()

	if err := os.MkdirAll(cfg.ImagesRoot, 0o755); err != nil {
		logger.Error("failed to create images root", "error", err.Error())
		os.Exit(1)
	}

	sessionManager, err := session.NewManager(session.Config{
		Root:          cfg.SessionsRoot,
		RootfsArchive: cfg.SessionRootfsArchive,
		Timeout:       cfg.SessionTimeout,
		SweepInterval: cfg.SessionSweepInterval,
		MaxSessions:   cfg.MaxSessions,
	}, logger, metrics)
	if err != nil {
		logger.Error("failed to create session manager", "error", err.Error())
		os.Exit(1)
	}

	trinoClient, err := trinoapp.NewClient(trinoapp.Config{
		Scheme:            cfg.TrinoScheme,
		Host:              cfg.TrinoHost,
		Port:              cfg.TrinoPort,
		Catalog:           cfg.TrinoCatalog,
		Schema:            cfg.TrinoSchema,
		Source:            cfg.TrinoSource,
		MaxInlineRows:     cfg.MaxInlineRows,
		MaxQueryFileBytes: cfg.MaxQueryFileBytes,
	}, logger, metrics)
	if err != nil {
		logger.Error("failed to create trino client", "error", err.Error())
		os.Exit(1)
	}
	defer trinoClient.Close()

	runner := sandbox.NewRunner(sandbox.Config{
		DefaultTimeout: cfg.DefaultBashTimeout,
		MaxTimeout:     cfg.MaxBashTimeout,
		MaxOutputBytes: cfg.MaxBashOutputBytes,
	}, logger, metrics)

	mcpHandler := mcpserver.New(mcpserver.Dependencies{
		Config:         cfg,
		Logger:         logger,
		Metrics:        metrics,
		Sessions:       sessionManager,
		Sandbox:        runner,
		Trino:          trinoClient,
		ServerVersion:  version,
		ServerName:     "data-warehouse-mcp",
		ReadinessCheck: func(ctx context.Context) error { return trinoClient.Ready(ctx) },
	})

	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpHandler)
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", httpapi.HealthHandler)
	mux.HandleFunc("GET /images/{filename}", httpapi.ImageHandler(cfg.ImagesRoot))
	mux.HandleFunc("GET /readyz", httpapi.ReadinessHandler(func(ctx context.Context) error {
		if err := sessionManager.Ready(); err != nil {
			return err
		}
		readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return trinoClient.Ready(readyCtx)
	}))

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	sessionManager.Start(ctx)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", "error", err.Error())
		}
	}()

	logger.Info("starting data-warehouse-mcp",
		"version", version,
		"listen_addr", cfg.ListenAddr,
		"trino_host", cfg.TrinoHost,
		"trino_port", cfg.TrinoPort,
		"trino_catalog", cfg.TrinoCatalog,
		"trino_schema", cfg.TrinoSchema,
		"session_timeout", cfg.SessionTimeout.String(),
		"session_sweep_interval", cfg.SessionSweepInterval.String(),
		"max_sessions", cfg.MaxSessions,
	)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server failed", "error", err.Error())
		os.Exit(1)
	}

	if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("server stopped", "error", fmt.Sprintf("%v", err))
	}
}
