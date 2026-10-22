package main

import (
	"context"
	"distributed-payments-ledger/internal/httpapi"
	"distributed-payments-ledger/internal/observability"
	"distributed-payments-ledger/internal/platform"
	"distributed-payments-ledger/internal/service"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	ctx, cancel := platform.Context()
	defer cancel()
	c := platform.MustConfig()
	if c.APIKey == "" {
		slog.Error("API_KEY is required")
		os.Exit(1)
	}
	s, e := service.Open(ctx, c)
	if e != nil {
		slog.Error("database", "error", e)
		os.Exit(1)
	}
	defer s.Close()
	m := observability.New()
	m.Attach(s)
	go m.Serve(ctx, c.MetricsListen)
	a := &httpapi.API{Service: s, Key: c.APIKey, Metrics: m}
	srv := &http.Server{Addr: c.Listen, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 20 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	slog.Info("API listening", "address", c.Listen)
	if e = srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		slog.Error("HTTP server", "error", e)
		os.Exit(1)
	}
}
