package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"gatewright/internal/admin"
	_ "gatewright/internal/limiter/builtin"
	"gatewright/internal/config"
	"gatewright/internal/errs"
	"gatewright/internal/obs"
	"gatewright/internal/runtime"
)

func runCmd(args []string) {
	fs := flagSet("run")
	cfgPath := fs.String("c", "gateway.yaml", "configuration file")
	noColor := fs.Bool("no-color", false, "disable coloured terminal output")
	fs.Parse(args)

	cfg, verr := config.Load(*cfgPath)
	if verr != nil {
		fmt.Fprint(os.Stderr, verr.Error())
		os.Exit(1)
	}

	logger, err := runtime.NewLoggerFromConfig(cfg, *noColor, isTTY(os.Stdout))
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}

	metrics := obs.NewMetrics()
	sup, err := runtime.NewSupervisor(*cfgPath, logger, metrics, version)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	sup.StartSampler(ctx)
	sup.Watch(ctx, 500*time.Millisecond)

	// Gateway listener with reserved operational paths served locally.
	gwMux := http.NewServeMux()
	registerReserved(gwMux, cfg, metrics, sup)
	gwMux.Handle("/", sup.Handler())

	gwSrv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           gwMux,
		ReadTimeout:       cfg.Server.ReadTimeout.D,
		WriteTimeout:      0, // streaming responses outlive any fixed write budget
		IdleTimeout:       cfg.Server.IdleTimeout.D,
		MaxHeaderBytes:    int(cfg.Server.MaxHeaderBytes.N),
		ReadHeaderTimeout: 10 * time.Second,
	}

	adminToken := resolveAdminToken(cfg)
	if !loopbackOnly(cfg.Admin.Listen) && adminToken == "" {
		// Config validation already rejects this; belt and braces at boot.
		fmt.Fprintln(os.Stderr, "fatal: admin listens beyond loopback without auth")
		os.Exit(1)
	}
	adminHandler := admin.New(sup, admin.Options{AuthToken: adminToken, Dashboard: cfg.Admin.Dashboard != nil && *cfg.Admin.Dashboard})
	adminSrv := &http.Server{
		Addr:              cfg.Admin.Listen,
		Handler:           adminHandler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	var ready atomic.Bool
	ready.Store(true)

	errCh := make(chan error, 2)
	go func() {
		if cfg.Server.TLS != nil && (cfg.Server.TLS.CertFile != "" || cfg.Server.TLS.KeyFile != "") {
			tlsConf, terr := serverTLSConfig(cfg.Server.TLS, logger)
			if terr != nil {
				errCh <- terr
				return
			}
			gwSrv.TLSConfig = tlsConf
			ln, lerr := net.Listen("tcp", cfg.Server.Listen)
			if lerr != nil {
				errCh <- lerr
				return
			}
			tlsLn := tls.NewListener(ln, tlsConf)
			logger.Info("gateway listening (TLS)",
				"addr", cfg.Server.Listen,
				"min_version", cfg.Server.TLS.MinVersion,
				"admin", cfg.Admin.Listen)
			errCh <- gwSrv.Serve(tlsLn)
			return
		}
		logger.Info("gateway listening", "addr", cfg.Server.Listen, "admin", cfg.Admin.Listen)
		errCh <- gwSrv.ListenAndServe()
	}()
	go func() {
		errCh <- adminSrv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "fatal:", err)
			stop()
		}
	}

	logger.Info("shutting down: draining in-flight requests", "deadline", "30s")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = gwSrv.Shutdown(shutdownCtx)
	_ = adminSrv.Shutdown(shutdownCtx)
	sup.Drain(30 * time.Second)
	logger.Info("shutdown complete")
}

func registerReserved(mux *http.ServeMux, cfg *config.Config, metrics *obs.Metrics, sup *runtime.Supervisor) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","version":"` + version + `"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		unready := unreadyPools(sup)
		w.Header().Set("Content-Type", "application/json")
		if len(unready) > 0 {
			// Opaque on purpose: pool names are internal topology and must
			// not leak through an unauthenticated probe endpoint.
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unavailable"}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	if cfg.Observability.Metrics.EnabledOrDefault() {
		mux.Handle(cfg.Observability.Metrics.Path, metrics.Handler())
	}
}

func unreadyPools(sup *runtime.Supervisor) []string { return sup.UnhealthyPools() }

func loopbackOnly(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if host == "" || host == "localhost" {
		return true
	}
	return ip != nil && ip.IsLoopback()
}

func resolveAdminToken(cfg *config.Config) string {
	if cfg.Admin.Auth.TokenEnv != "" {
		return os.Getenv(cfg.Admin.Auth.TokenEnv)
	}
	if cfg.Admin.Auth.TokenFile != "" {
		data, err := os.ReadFile(cfg.Admin.Auth.TokenFile)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(data))
	}
	// Fail-safe default: when no source is configured, GATEWRIGHT_ADMIN_TOKEN
	// is honoured automatically so a non-loopback bind can never go unauthenticated.
	return os.Getenv("GATEWRIGHT_ADMIN_TOKEN")
}

func serverTLSConfig(st *config.ServerTLS, logger obs.Logger) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(st.CertFile, st.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("server.tls: %w", err)
	}
	min := uint16(tls.VersionTLS12)
	switch st.MinVersion {
	case "tls13":
		min = tls.VersionTLS13
	default:
		// Config validation restricts min_version to tls12|tls13; anything
		// else never reaches the running gateway.
		min = tls.VersionTLS12
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   min,
	}, nil
}

func writeErr(w http.ResponseWriter, apiErr *errs.APIError) { errs.Write(w, apiErr) }
