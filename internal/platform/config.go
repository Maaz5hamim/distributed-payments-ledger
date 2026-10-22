package platform

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	Coordinator                                 string
	Shards                                      []string
	APIKey, Listen, MetricsListen, Kafka, Group string
	MaxAttempts                                 int
}

func Load() (Config, error) {
	c := Config{Coordinator: os.Getenv("COORDINATOR_URL"), Shards: strings.Split(os.Getenv("SHARD_URLS"), ","), APIKey: os.Getenv("API_KEY"), Listen: env("LISTEN_ADDR", ":8080"), MetricsListen: env("METRICS_ADDR", ":9090"), Kafka: env("KAFKA_BROKERS", "kafka:9092"), Group: env("KAFKA_GROUP", "ledger-audit"), MaxAttempts: 8}
	if c.Coordinator == "" || len(c.Shards) == 0 || c.Shards[0] == "" {
		return c, errors.New("COORDINATOR_URL and ordered SHARD_URLS are required")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func Context() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
func Pool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	c, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, e
	}
	c.MaxConns = 16
	c.ConnConfig.ConnectTimeout = 3 * time.Second
	c.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	c.ConnConfig.RuntimeParams["lock_timeout"] = "3000"
	return pgxpool.NewWithConfig(ctx, c)
}
func MustConfig() Config {
	c, e := Load()
	if e != nil {
		slog.Error("configuration", "error", e)
		os.Exit(1)
	}
	return c
}
