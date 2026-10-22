package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-payments-ledger/internal/httpapi"
	"distributed-payments-ledger/internal/ledger"
	"distributed-payments-ledger/internal/observability"
	"distributed-payments-ledger/internal/outbox"
	"distributed-payments-ledger/internal/platform"
	"distributed-payments-ledger/internal/service"
	"distributed-payments-ledger/internal/shard"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"
)

func setup(t *testing.T) *service.Service {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, base)
	if e != nil {
		t.Fatal(e)
	}
	schema := "test_" + strings.ReplaceAll(ledger.ID(), "-", "")
	urls := []string{}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("%s_%d", schema, i)
		if _, e = admin.Exec(ctx, "CREATE SCHEMA "+name); e != nil {
			t.Fatal(e)
		}
		sep := "?"
		if strings.Contains(base, "?") {
			sep = "&"
		}
		urls = append(urls, base+sep+"search_path="+name)
	}
	s, e := service.Open(ctx, platform.Config{Coordinator: urls[0], Shards: urls[1:], MaxAttempts: 2})
	if e != nil {
		t.Fatal(e)
	}
	for i, p := range append([]*pgxpool.Pool{s.Coord}, s.Shards...) {
		path := "shard"
		if i == 0 {
			path = "coordinator"
		}
		sql, e := os.ReadFile(filepath.Join("..", "..", "db", path, "001_init.sql"))
		if e != nil {
			t.Fatal(e)
		}
		if _, e = p.Exec(ctx, string(sql)); e != nil {
			t.Fatal(e)
		}
	}
	t.Cleanup(func() {
		s.Close()
		for i := 0; i < 3; i++ {
			admin.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s_%d CASCADE", schema, i))
		}
		admin.Close()
	})
	return s
}
func account(t *testing.T, s *service.Service, index int, balance int64) string {
	t.Helper()
	for {
		id := ledger.ID()
		if shard.Route(id, len(s.Shards)) == index {
			if e := ledger.Create(context.Background(), s.Shards[index], ledger.ID(), id, balance); e != nil {
				t.Fatal(e)
			}
			return id
		}
	}
}
func job(t *testing.T, s *service.Service, from, to string, n int64) service.Job {
	t.Helper()
	j := service.Job{ID: ledger.ID(), Kind: "transfer", From: from, To: to, Amount: n, FromShard: shard.Route(from, len(s.Shards)), ToShard: shard.Route(to, len(s.Shards))}
	j, e := s.Submit(context.Background(), j.ID, j.ID, j)
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func drain(t *testing.T, s *service.Service, id string) service.Job {
	t.Helper()
	for i := 0; i < 30; i++ {
		j, e := s.Get(context.Background(), id)
		if e != nil {
			t.Fatal(e)
		}
		if service.Terminal(j.State) {
			return j
		}
		s.Coord.Exec(context.Background(), "UPDATE jobs SET next_attempt=now() WHERE id=$1", id)
		if _, e = s.Process(context.Background(), id); e != nil {
			t.Fatal(e)
		}
	}
	t.Fatal("job did not finish")
	return service.Job{}
}
func balance(t *testing.T, s *service.Service, id string) int64 {
	t.Helper()
	var b int64
	if e := s.Shards[shard.Route(id, len(s.Shards))].QueryRow(context.Background(), "SELECT balance FROM accounts WHERE id=$1", id).Scan(&b); e != nil {
		t.Fatal(e)
	}
	return b
}
func audit(t *testing.T, s *service.Service) {
	t.Helper()
	m := observability.New()
	if e := m.Reconcile(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if n := testutil.ToFloat64(m.Mismatch); n != 0 {
		t.Fatalf("%g reconciliation mismatches", n)
	}
}
func TestLocalAndCrossShard(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	b := account(t, s, 0, 0)
	c := account(t, s, 1, 0)
	for _, to := range []string{b, c} {
		j := drain(t, s, job(t, s, a, to, 25).ID)
		if j.State != "COMPLETED" {
			t.Fatal(j)
		}
		drain(t, s, j.ID)
	}
	if balance(t, s, a) != 50 || balance(t, s, b) != 25 || balance(t, s, c) != 25 {
		t.Fatal("wrong balances")
	}
	audit(t, s)
}
func TestCrashAfterDebitCommit(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	b := account(t, s, 1, 0)
	j := job(t, s, a, b, 25)
	// Simulate a shard commit followed by worker death before coordinator update.
	if e := ledger.Apply(context.Background(), s.Shards[0], j.ID, "debit", a, ledger.Clearing, 25); e != nil {
		t.Fatal(e)
	}
	audit(t, s)
	if j = drain(t, s, j.ID); j.State != "COMPLETED" {
		t.Fatal(j)
	}
	if balance(t, s, a) != 75 || balance(t, s, b) != 25 {
		t.Fatal("duplicate debit")
	}
	audit(t, s)
}
func TestCompensationAndFence(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	b := account(t, s, 1, 0)
	s.Shards[1].Exec(context.Background(), "DELETE FROM accounts WHERE id=$1", b)
	j := drain(t, s, job(t, s, a, b, 25).ID)
	if j.State != "REVERSED" || balance(t, s, a) != 100 {
		t.Fatal(j)
	}
	e := ledger.Apply(context.Background(), s.Shards[1], j.ID, "credit", ledger.Clearing, b, 25)
	if !errors.Is(e, ledger.ErrCancelled) {
		t.Fatalf("late credit was not fenced: %v", e)
	}
	audit(t, s)
}
func TestAmbiguousCreditCompletesInsteadOfRefund(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	b := account(t, s, 1, 0)
	j := job(t, s, a, b, 25)
	ctx := context.Background()
	if e := ledger.Apply(ctx, s.Shards[0], j.ID, "debit", a, ledger.Clearing, 25); e != nil {
		t.Fatal(e)
	}
	if e := ledger.Apply(ctx, s.Shards[1], j.ID, "credit", ledger.Clearing, b, 25); e != nil {
		t.Fatal(e)
	}
	s.Coord.Exec(ctx, "UPDATE jobs SET state='CANCELLING' WHERE id=$1", j.ID)
	j = drain(t, s, j.ID)
	if j.State != "COMPLETED" || balance(t, s, a) != 75 || balance(t, s, b) != 25 {
		t.Fatal(j)
	}
	audit(t, s)
}
func TestConcurrentOverdraft(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 50)
	b := account(t, s, 0, 0)
	var success atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := ledger.Apply(context.Background(), s.Shards[0], ledger.ID(), "local", a, b, 1)
			if e == nil {
				success.Add(1)
			} else if !errors.Is(e, ledger.ErrFunds) {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if success.Load() != 50 || balance(t, s, a) != 0 || balance(t, s, b) != 50 {
		t.Fatal("overdraft or lost movement")
	}
	audit(t, s)
}
func TestCreditCancellationRace(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	b := account(t, s, 1, 0)
	for i := 0; i < 20; i++ {
		j := job(t, s, a, b, 1)
		if e := ledger.Apply(context.Background(), s.Shards[0], j.ID, "debit", a, ledger.Clearing, 1); e != nil {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			e := ledger.Apply(context.Background(), s.Shards[1], j.ID, "credit", ledger.Clearing, b, 1)
			if e != nil && !errors.Is(e, ledger.ErrCancelled) {
				t.Error(e)
			}
		}()
		go func() {
			defer wg.Done()
			if _, e := ledger.CancelCredit(context.Background(), s.Shards[1], j.ID); e != nil {
				t.Error(e)
			}
		}()
		wg.Wait()
		s.Coord.Exec(context.Background(), "UPDATE jobs SET state='CANCELLING' WHERE id=$1", j.ID)
		drain(t, s, j.ID)
	}
	if balance(t, s, a)+balance(t, s, b) != 100 {
		t.Fatal("money changed")
	}
	audit(t, s)
}
func TestRandomTransfers(t *testing.T) {
	s := setup(t)
	ids := []string{}
	for i := 0; i < 10; i++ {
		ids = append(ids, account(t, s, i%2, 100))
	}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 200; i++ {
		from, to := rng.Intn(10), rng.Intn(10)
		if from == to {
			continue
		}
		drain(t, s, job(t, s, ids[from], ids[to], int64(rng.Intn(150)+1)).ID)
	}
	sum := int64(0)
	for _, id := range ids {
		b := balance(t, s, id)
		if b < 0 {
			t.Fatal("negative customer")
		}
		sum += b
	}
	if sum != 1000 {
		t.Fatalf("money changed: %d", sum)
	}
	audit(t, s)
}
func TestStepRetries(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	b := account(t, s, 1, 0)
	j := job(t, s, a, b, 10)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if e := ledger.Apply(ctx, s.Shards[0], j.ID, "debit", a, ledger.Clearing, 10); e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 3; i++ {
		if e := ledger.Apply(ctx, s.Shards[1], j.ID, "credit", ledger.Clearing, b, 10); e != nil {
			t.Fatal(e)
		}
	}
	drain(t, s, j.ID)
	if balance(t, s, a) != 90 || balance(t, s, b) != 10 {
		t.Fatal("step applied more than once")
	}
	audit(t, s)
}

type publisher struct {
	fail     bool
	messages []kafka.Message
}

func (p *publisher) WriteMessages(_ context.Context, m ...kafka.Message) error {
	p.messages = append(p.messages, m...)
	if p.fail {
		return errors.New("lost publish acknowledgement")
	}
	return nil
}
func TestOutboxRecoveryAndDedup(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 10)
	b := account(t, s, 0, 0)
	drain(t, s, job(t, s, a, b, 1).ID)
	ctx := context.Background()
	p := &publisher{fail: true}
	if _, e := outbox.RelayOne(ctx, s.Shards[0], p); e == nil {
		t.Fatal("expected publication failure")
	}
	p.fail = false
	if _, e := outbox.RelayOne(ctx, s.Shards[0], p); e != nil {
		t.Fatal(e)
	}
	if len(p.messages) != 2 || !bytes.Equal(p.messages[0].Value, p.messages[1].Value) {
		t.Fatal("event changed on retry")
	}
	for _, m := range p.messages {
		if e := outbox.Consume(ctx, s.Coord, m.Value); e != nil {
			t.Fatal(e)
		}
	}
	var n int
	if e := s.Coord.QueryRow(ctx, "SELECT count(*) FROM consumed_events").Scan(&n); e != nil || n != 1 {
		t.Fatalf("dedup: %d %v", n, e)
	}
}
func TestHTTPIdempotency(t *testing.T) {
	s := setup(t)
	m := observability.New()
	a := httpapi.API{Service: s, Key: "test", Metrics: m}
	h := a.Handler()
	request := func(path, key, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.Header.Set("X-API-Key", "test")
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	first := request("/accounts", "same", `{"initial_deposit":100}`)
	second := request("/accounts", "same", `{ "initial_deposit":100 }`)
	if first.Code != 201 || second.Code != first.Code || first.Body.String() != second.Body.String() {
		t.Fatalf("replay: %d %s / %d %s", first.Code, first.Body, second.Code, second.Body)
	}
	if w := request("/accounts", "same", `{"initial_deposit":200}`); w.Code != 422 {
		t.Fatal(w.Code)
	}
	if w := request("/accounts", "null", `null`); w.Code != 400 {
		t.Fatal("null body accepted")
	}
	if w := request("/accounts", "other", `{} {}`); w.Code != 400 {
		t.Fatal(w.Code)
	}
	r := httptest.NewRequest("GET", "/accounts/"+ledger.ID(), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("missing auth accepted")
	}
	var v map[string]any
	json.Unmarshal(first.Body.Bytes(), &v)
	r = httptest.NewRequest("GET", "/accounts/"+v["id"].(string)+"/entries?cursor=-1", nil)
	r.Header.Set("X-API-Key", "test")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("negative cursor accepted")
	}
	audit(t, s)
}
func TestConcurrentIdempotency(t *testing.T) {
	s := setup(t)
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j := service.Job{ID: ledger.ID(), Kind: "account", To: ledger.ID()}
			j, e := s.Submit(context.Background(), "shared", "hash", j)
			if e == nil {
				ids <- j.ID
			} else if !errors.Is(e, service.ErrBusy) {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first != "" && id != first {
			t.Fatal("multiple intents")
		}
		first = id
	}
	var n int
	s.Coord.QueryRow(context.Background(), "SELECT count(*) FROM jobs").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
}
func TestWorkerRecoveryMultipleReplicas(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	b := account(t, s, 1, 0)
	jobs := []string{}
	for i := 0; i < 30; i++ {
		jobs = append(jobs, job(t, s, a, b, 1).ID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.Run(ctx) }()
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, id := range jobs {
			j, e := s.Get(context.Background(), id)
			if e != nil {
				t.Fatal(e)
			}
			done = done && service.Terminal(j.State)
		}
		if done {
			cancel()
			wg.Wait()
			if balance(t, s, a) != 70 || balance(t, s, b) != 30 {
				t.Fatal("duplicate execution")
			}
			audit(t, s)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	t.Fatal("workers did not finish")
}

func TestOverflowReversesWithoutLosingFunds(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 1)
	b := account(t, s, 1, int64(9223372036854775807))
	j := drain(t, s, job(t, s, a, b, 1).ID)
	if j.State != "REVERSED" || balance(t, s, a) != 1 || balance(t, s, b) != 9223372036854775807 {
		t.Fatal(j)
	}
	audit(t, s)
}
func TestAccountCreationRecoversAfterCommit(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	id := ledger.ID()
	index := shard.Route(id, 2)
	j, e := s.Submit(ctx, "account-crash", "hash", service.Job{ID: ledger.ID(), Kind: "account", To: id, Amount: 100, FromShard: index, ToShard: index})
	if e != nil {
		t.Fatal(e)
	}
	if e = ledger.Create(ctx, s.Shards[index], j.ID, id, 100); e != nil {
		t.Fatal(e)
	}
	if drain(t, s, j.ID).State != "COMPLETED" || balance(t, s, id) != 100 {
		t.Fatal("account creation duplicated")
	}
	audit(t, s)
}
func TestReconciliationDetectsDrift(t *testing.T) {
	s := setup(t)
	a := account(t, s, 0, 100)
	if _, e := s.Shards[0].Exec(context.Background(), "UPDATE accounts SET balance=99 WHERE id=$1", a); e != nil {
		t.Fatal(e)
	}
	m := observability.New()
	if e := m.Reconcile(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if testutil.ToFloat64(m.Mismatch) == 0 {
		t.Fatal("corruption was not detected")
	}
}
