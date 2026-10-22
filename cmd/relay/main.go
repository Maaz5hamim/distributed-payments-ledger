package main

import (
	"context"
	"distributed-payments-ledger/internal/outbox"
	"distributed-payments-ledger/internal/platform"
	"distributed-payments-ledger/internal/service"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"os"
	"strings"
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
	w := &kafka.Writer{Addr: kafka.TCP(strings.Split(c.Kafka, ",")...), Topic: "transfer-events", Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchSize: 1, WriteTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second, AllowAutoTopicCreation: false}
	defer w.Close()
	for ctx.Err() == nil {
		any := false
		for i, p := range s.Shards {
			step, stop := context.WithTimeout(ctx, 4*time.Second)
			worked, e := outbox.RelayOne(step, p, w)
			stop()
			if worked && e == nil {
				any = true
			}
			if e != nil {
				slog.Warn("relay retry", "shard", i, "error", e)
			}
		}
		if !any {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}
