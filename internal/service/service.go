package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"distributed-payments-ledger/internal/ledger"
	"distributed-payments-ledger/internal/platform"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	Coord       *pgxpool.Pool
	Shards      []*pgxpool.Pool
	MaxAttempts int
	Observe     func(Job, string)
}
type Job struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	From      string    `json:"from_account,omitempty"`
	To        string    `json:"to_account"`
	Amount    int64     `json:"amount"`
	FromShard int       `json:"from_shard"`
	ToShard   int       `json:"to_shard"`
	State     string    `json:"state"`
	Attempts  int       `json:"attempts"`
	Error     string    `json:"error,omitempty"`
	Created   time.Time `json:"created_at"`
	Updated   time.Time `json:"updated_at"`
}

const columns = "id::text,kind,COALESCE(from_account::text,''),to_account::text,amount,from_shard,to_shard,state,attempts,error,created_at,updated_at"

func scan(row pgx.Row) (Job, error) {
	var j Job
	e := row.Scan(&j.ID, &j.Kind, &j.From, &j.To, &j.Amount, &j.FromShard, &j.ToShard, &j.State, &j.Attempts, &j.Error, &j.Created, &j.Updated)
	return j, e
}
func Open(ctx context.Context, c platform.Config) (*Service, error) {
	s := &Service{MaxAttempts: c.MaxAttempts}
	var e error
	s.Coord, e = platform.Pool(ctx, c.Coordinator)
	if e != nil {
		return nil, e
	}
	for _, url := range c.Shards {
		p, e := platform.Pool(ctx, url)
		if e != nil {
			s.Close()
			return nil, e
		}
		s.Shards = append(s.Shards, p)
	}
	return s, nil
}
func (s *Service) Close() {
	if s.Coord != nil {
		s.Coord.Close()
	}
	for _, p := range s.Shards {
		p.Close()
	}
}
func Terminal(state string) bool {
	return state == "COMPLETED" || state == "FAILED" || state == "REVERSED"
}
func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	return scan(s.Coord.QueryRow(ctx, "SELECT "+columns+" FROM jobs WHERE id=$1", id))
}

var ErrConflict = errors.New("idempotency key reused with different request")
var ErrBusy = errors.New("request in progress")

// Submit commits intent and the key together before any shard is touched.
func (s *Service) Submit(ctx context.Context, key, hash string, j Job) (Job, error) {
	tx, e := s.Coord.Begin(ctx)
	if e != nil {
		return j, e
	}
	defer tx.Rollback(ctx)
	var locked bool
	if e = tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", key).Scan(&locked); e != nil {
		return j, e
	}
	if !locked {
		return j, ErrBusy
	}
	var oldHash, id string
	e = tx.QueryRow(ctx, "SELECT request_hash,job_id::text FROM idempotency_keys WHERE key=$1", key).Scan(&oldHash, &id)
	if e == nil {
		if oldHash != hash {
			return j, ErrConflict
		}
		return s.Get(ctx, id)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return j, e
	}
	var from any
	if j.From != "" {
		from = j.From
	}
	_, e = tx.Exec(ctx, `INSERT INTO jobs(id,kind,from_account,to_account,amount,from_shard,to_shard) VALUES($1,$2,$3,$4,$5,$6,$7)`, j.ID, j.Kind, from, j.To, j.Amount, j.FromShard, j.ToShard)
	if e != nil {
		return j, e
	}
	_, e = tx.Exec(ctx, "INSERT INTO idempotency_keys(key,request_hash,job_id) VALUES($1,$2,$3)", key, hash, j.ID)
	if e != nil {
		return j, e
	}
	if e = tx.Commit(ctx); e != nil {
		return j, e
	}
	return s.Get(ctx, j.ID)
}

// Process owns a coordinator row lock for one bounded shard step. Locks are
// released automatically on worker death. All remote effects are retry safe.
func (s *Service) Process(ctx context.Context, id string) (bool, error) {
	tx, e := s.Coord.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared($1)", int64(710281004)); e != nil {
		return false, e
	}
	query := "SELECT " + columns + " FROM jobs WHERE state NOT IN ('COMPLETED','REVERSED','FAILED') AND next_attempt<=now()"
	var args []any
	if id != "" {
		query += " AND id=$1"
		args = append(args, id)
	}
	query += " ORDER BY next_attempt,created_at LIMIT 1 FOR UPDATE SKIP LOCKED"
	j, e := scan(tx.QueryRow(ctx, query, args...))
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	stepCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	state, stepErr := s.advance(stepCtx, j)
	attempts := 0
	message := ""
	delay := time.Duration(0)
	if stepErr != nil {
		message = stepErr.Error()
		attempts = j.Attempts + 1
		delay = Backoff(attempts)
		state = j.State
		if ledger.Permanent(stepErr) {
			switch j.State {
			case "PENDING":
				state = "FAILED"
			case "DEBITED":
				state = "CANCELLING"
			}
		}
		if j.State == "DEBITED" && attempts >= s.MaxAttempts {
			state = "CANCELLING"
		}
		slog.Warn("saga retry", "id", j.ID, "state", state, "error", stepErr)
	}
	if !AllowedTransition(j.State, state) {
		return true, fmt.Errorf("illegal transition %s -> %s", j.State, state)
	}
	_, e = tx.Exec(ctx, `UPDATE jobs SET state=$2,attempts=$3,total_attempts=total_attempts+1,error=$4,next_attempt=now()+$5::interval,updated_at=now() WHERE id=$1`, j.ID, state, attempts, message, fmt.Sprintf("%f seconds", delay.Seconds()))
	if e != nil {
		return true, e
	}
	if e = tx.Commit(ctx); e == nil && s.Observe != nil {
		s.Observe(j, state)
	}
	return true, e
}
func Backoff(attempt int) time.Duration {
	if attempt > 6 {
		attempt = 6
	}
	return time.Duration(1<<attempt) * 100 * time.Millisecond
}
func (s *Service) advance(ctx context.Context, j Job) (string, error) {
	source, target := s.Shards[j.FromShard], s.Shards[j.ToShard]
	if j.Kind == "account" {
		e := ledger.Create(ctx, target, j.ID, j.To, j.Amount)
		return "COMPLETED", e
	}
	if j.FromShard == j.ToShard {
		e := ledger.Apply(ctx, source, j.ID, "local", j.From, j.To, j.Amount)
		return "COMPLETED", e
	}
	switch j.State {
	case "PENDING":
		e := ledger.Apply(ctx, source, j.ID, "debit", j.From, ledger.Clearing, j.Amount)
		return "DEBITED", e
	case "DEBITED":
		e := ledger.Apply(ctx, target, j.ID, "credit", ledger.Clearing, j.To, j.Amount)
		return "COMPLETING", e
	case "CANCELLING":
		applied, e := ledger.CancelCredit(ctx, target, j.ID)
		if applied {
			return "COMPLETING", e
		}
		return "COMPENSATING", e
	case "COMPENSATING":
		e := ledger.Apply(ctx, source, j.ID, "compensate", ledger.Clearing, j.From, j.Amount)
		return "REVERSING", e
	case "COMPLETING":
		e := ledger.Emit(ctx, source, j.ID, "COMPLETED", j.From, j.To, j.Amount)
		return "COMPLETED", e
	case "REVERSING":
		e := ledger.Emit(ctx, source, j.ID, "REVERSED", j.From, j.To, j.Amount)
		return "REVERSED", e
	}
	return j.State, fmt.Errorf("invalid state %s", j.State)
}

// Response is persisted once, so retries replay even an original 202 after the
// job finishes. GET /transfers/{id} exposes its current state.
func (s *Service) Response(ctx context.Context, id string) (int, json.RawMessage, error) {
	tx, e := s.Coord.Begin(ctx)
	if e != nil {
		return 0, nil, e
	}
	defer tx.Rollback(ctx)
	j, e := scan(tx.QueryRow(ctx, "SELECT "+columns+" FROM jobs WHERE id=$1 FOR UPDATE", id))
	if e != nil {
		return 0, nil, e
	}
	var code *int
	var data []byte
	if e = tx.QueryRow(ctx, "SELECT status_code,response FROM jobs WHERE id=$1", id).Scan(&code, &data); e != nil {
		return 0, nil, e
	}
	if code != nil {
		return *code, data, nil
	}
	status := 202
	var value any = j
	if j.Kind == "account" {
		value = map[string]any{"id": j.To, "operation_id": j.ID, "state": j.State}
		if j.State == "COMPLETED" {
			status = 201
		}
	}
	if j.Kind == "transfer" && j.State == "COMPLETED" && j.FromShard == j.ToShard {
		status = 201
	}
	if j.State == "FAILED" {
		status = 409
	}
	data, e = json.Marshal(value)
	if e != nil {
		return 0, nil, e
	}
	e = tx.QueryRow(ctx, "UPDATE jobs SET response=$2,status_code=$3 WHERE id=$1 RETURNING response", id, string(data), status).Scan(&data)
	if e != nil {
		return 0, nil, e
	}
	return status, data, tx.Commit(ctx)
}
func (s *Service) Run(ctx context.Context) {
	for ctx.Err() == nil {
		worked, e := s.Process(ctx, "")
		if e != nil {
			slog.Error("worker", "error", e)
		}
		if !worked || e != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

// AllowedTransition is deliberately explicit: retries can retain a nonterminal
// state, but terminal outcomes never transition back into work.
func AllowedTransition(from, to string) bool {
	if Terminal(from) {
		return false
	}
	if from == to {
		switch from {
		case "PENDING", "DEBITED", "CANCELLING", "COMPENSATING", "COMPLETING", "REVERSING":
			return true
		}
		return false
	}
	switch from {
	case "PENDING":
		return to == "DEBITED" || to == "COMPLETED" || to == "FAILED"
	case "DEBITED":
		return to == "COMPLETING" || to == "CANCELLING"
	case "CANCELLING":
		return to == "COMPLETING" || to == "COMPENSATING"
	case "COMPENSATING":
		return to == "REVERSING"
	case "COMPLETING":
		return to == "COMPLETED"
	case "REVERSING":
		return to == "REVERSED"
	}
	return false
}
