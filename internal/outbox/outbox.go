package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

// Delivery is at least once. Never acknowledge a row until Kafka acknowledges
// it; consumer side effects and event deduplication must share a transaction.
func RelayOne(ctx context.Context, p *pgxpool.Pool, w Publisher) (bool, error) {
	tx, e := p.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	var id int64
	var transfer string
	var payload []byte
	e = tx.QueryRow(ctx, "SELECT id,transfer_id::text,payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED").Scan(&id, &transfer, &payload)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if e = w.WriteMessages(ctx, kafka.Message{Key: []byte(transfer), Value: payload}); e != nil {
		return true, e
	}
	if _, e = tx.Exec(ctx, "UPDATE outbox SET sent_at=now() WHERE id=$1", id); e != nil {
		return true, e
	}
	return true, tx.Commit(ctx)
}
func Consume(ctx context.Context, p *pgxpool.Pool, payload []byte) error {
	var event struct {
		ID string `json:"event_id"`
	}
	if e := json.Unmarshal(payload, &event); e != nil {
		return e
	}
	if event.ID == "" {
		return errors.New("missing event_id")
	}
	_, e := p.Exec(ctx, "INSERT INTO consumed_events(event_id,payload) VALUES($1,$2) ON CONFLICT(event_id) DO NOTHING", event.ID, string(payload))
	return e
}
