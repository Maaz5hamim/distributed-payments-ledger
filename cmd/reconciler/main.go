package main

import (
	"context"
	"distributed-payments-ledger/internal/observability"
	"distributed-payments-ledger/internal/platform"
	"distributed-payments-ledger/internal/service"
	"log/slog"
	"os"
	"time"
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
	for ctx.Err() == nil {
		check, stop := context.WithTimeout(ctx, 12*time.Second)
		e = m.Reconcile(check, s)
		stop()
		if e != nil {
			slog.Error("reconciliation failed", "error", e)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
	}
}
