package main

import (
	"distributed-payments-ledger/internal/observability"
	"distributed-payments-ledger/internal/platform"
	"distributed-payments-ledger/internal/service"
	"log/slog"
	"os"
)

func main() {
	ctx, cancel := platform.Context()
	defer cancel()
	c := platform.MustConfig()
	s, e := service.Open(ctx, c)
	if e != nil {
		slog.Error("database", "error", e)
		os.Exit(1)
	}
	defer s.Close()
	m := observability.New()
	go m.Serve(ctx, c.MetricsListen)
	m.Attach(s)
	slog.Info("saga worker started")
	s.Run(ctx)
}
