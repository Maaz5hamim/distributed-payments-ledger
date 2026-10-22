package main

import (
	"distributed-payments-ledger/internal/outbox"
	"distributed-payments-ledger/internal/platform"
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
	p, e := platform.Pool(ctx, c.Coordinator)
	if e != nil {
		slog.Error("database", "error", e)
		os.Exit(1)
	}
	defer p.Close()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: strings.Split(c.Kafka, ","), Topic: "transfer-events", GroupID: c.Group, CommitInterval: 0, MaxWait: time.Second})
	defer r.Close()
	for ctx.Err() == nil {
		m, e := r.FetchMessage(ctx)
		if e != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("fetch", "error", e)
			continue
		}
		for ctx.Err() == nil {
			e = outbox.Consume(ctx, p, m.Value)
			if e == nil {
				e = r.CommitMessages(ctx, m)
			}
			if e == nil {
				break
			}
			slog.Warn("consumer retry", "error", e)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
}
