package main

import (
	"context"
	"distributed-payments-ledger/internal/platform"
	"log/slog"
	"os"
)

func main() {
	c := platform.MustConfig()
	ctx := context.Background()
	for i, url := range append([]string{c.Coordinator}, c.Shards...) {
		path := "db/shard/001_init.sql"
		if i == 0 {
			path = "db/coordinator/001_init.sql"
		}
		sql, e := os.ReadFile(path)
		if e != nil {
			panic(e)
		}
		p, e := platform.Pool(ctx, url)
		if e != nil {
			panic(e)
		}
		tx, e := p.Begin(ctx)
		if e != nil {
			panic(e)
		}
		if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(710281005)"); e != nil {
			panic(e)
		}
		if _, e = tx.Exec(ctx, string(sql)); e != nil {
			panic(e)
		}
		if e = tx.Commit(ctx); e != nil {
			panic(e)
		}
		p.Close()
		slog.Info("schema initialized", "database", i)
	}
}
