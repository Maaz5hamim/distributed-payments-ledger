package observability

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"math/big"
	"net/http"
	"time"

	"distributed-payments-ledger/internal/ledger"
	"distributed-payments-ledger/internal/service"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Transfers                             *prometheus.CounterVec
	Completion                            *prometheus.HistogramVec
	Registry                              *prometheus.Registry
	Duration                              *prometheus.HistogramVec
	Requests                              *prometheus.CounterVec
	Mismatch, Success, LastSuccess, Stuck prometheus.Gauge
	Unsent, Lag                           *prometheus.GaugeVec
	States                                *prometheus.GaugeVec
	Clearing                              prometheus.Gauge
}

func New() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(), Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "ledger_transfer_duration_seconds", Buckets: prometheus.DefBuckets}, []string{"path", "outcome"}), Requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ledger_requests_total"}, []string{"route", "status"}), Mismatch: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_reconciliation_mismatches"}), Success: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_reconciliation_success"}), LastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_reconciliation_last_success_timestamp_seconds"}), Stuck: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_saga_stuck"}), Unsent: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ledger_outbox_unsent_rows"}, []string{"shard"}), Lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ledger_outbox_lag_seconds"}, []string{"shard"}), States: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "ledger_sagas"}, []string{"state"}), Clearing: prometheus.NewGauge(prometheus.GaugeOpts{Name: "ledger_clearing_balance_minor_units"})}
	m.Transfers = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ledger_transfers_total"}, []string{"path", "outcome"})
	m.Completion = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "ledger_saga_completion_seconds", Buckets: []float64{.01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 120, 300}}, []string{"path", "outcome"})
	m.Registry.MustRegister(m.Transfers, m.Completion, m.Duration, m.Requests, m.Mismatch, m.Success, m.LastSuccess, m.Stuck, m.Unsent, m.Lag, m.States, m.Clearing, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
func (m *Metrics) Serve(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }()
	if e := srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		slog.Error("metrics server", "error", e)
	}
}

// Every mutation worker holds the shared barrier. The exclusive barrier pauses
// ledger writes while collecting independent shard snapshots. This avoids
// false clearing drift during cross-shard commits; reconciliation is intentionally
// bounded and briefly blocks workers. API intent submission can continue.
const Barrier = int64(710281004)

func (m *Metrics) Reconcile(ctx context.Context, s *service.Service) error {
	m.Success.Set(0)
	tx, e := s.Coord.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", Barrier); e != nil {
		return e
	}
	mismatches := int64(0)
	sum := new(big.Int)
	clearing := new(big.Int)
	type key struct{ id, step string }
	steps := map[key]bool{}
	for i, p := range s.Shards {
		st, e := p.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if e != nil {
			return e
		}
		a, e := ledger.Reconcile(ctx, st)
		if e != nil {
			st.Rollback(ctx)
			return e
		}
		mismatches += a.Mismatches
		v, _ := new(big.Int).SetString(a.Sum, 10)
		sum.Add(sum, v)
		v, _ = new(big.Int).SetString(a.Clearing, 10)
		clearing.Add(clearing, v)
		rows, e := st.Query(ctx, "SELECT transfer_id::text,step FROM steps WHERE status='APPLIED'")
		if e != nil {
			st.Rollback(ctx)
			return e
		}
		for rows.Next() {
			var id, step string
			if e = rows.Scan(&id, &step); e != nil {
				break
			}
			steps[key{id, step}] = true
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		st.Rollback(ctx)
		if e != nil {
			return e
		}
		label := big.NewInt(int64(i)).String()
		m.Unsent.WithLabelValues(label).Set(float64(a.Unsent))
		m.Lag.WithLabelValues(label).Set(a.Lag)
	}
	if sum.Sign() != 0 {
		mismatches++
	}
	expected := new(big.Int)
	states := map[string]int{}
	stuck := 0
	rows, e := tx.Query(ctx, `SELECT id::text,kind,from_shard,to_shard,amount,state,created_at FROM jobs`)
	if e != nil {
		return e
	}
	for rows.Next() {
		var id, kind, state string
		var from, to int
		var amount int64
		var created time.Time
		if e = rows.Scan(&id, &kind, &from, &to, &amount, &state, &created); e != nil {
			break
		}
		states[state]++
		if !service.Terminal(state) && time.Since(created) > time.Minute {
			stuck++
		}
		if kind != "transfer" || from == to {
			continue
		}
		d, c, r := steps[key{id, "debit"}], steps[key{id, "credit"}], steps[key{id, "compensate"}]
		if d {
			expected.Add(expected, big.NewInt(amount))
		}
		if c {
			expected.Sub(expected, big.NewInt(amount))
		}
		if r {
			expected.Sub(expected, big.NewInt(amount))
		}
		if c && r || ((c || r) && !d) || (state == "COMPLETED" && (!d || !c || r)) || (state == "REVERSED" && (!d || c || !r)) || (state == "FAILED" && d) {
			mismatches++
		}
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	if expected.Cmp(clearing) != 0 {
		mismatches++
	}
	for _, state := range []string{"PENDING", "DEBITED", "CANCELLING", "COMPENSATING", "COMPLETING", "REVERSING", "COMPLETED", "REVERSED", "FAILED"} {
		m.States.WithLabelValues(state).Set(float64(states[state]))
	}
	f, _ := new(big.Float).SetInt(clearing).Float64()
	m.Clearing.Set(f)
	m.Stuck.Set(float64(stuck))
	m.Mismatch.Set(float64(mismatches))
	m.Success.Set(1)
	m.LastSuccess.Set(float64(time.Now().Unix()))
	return nil
}

// Attach observes terminal commits performed by this process, including the API
// local fast path. Metrics are operational counters; the durable ledger remains
// the source of truth after a process crash between commit and observation.
func (m *Metrics) Attach(s *service.Service) {
	s.Observe = func(j service.Job, state string) {
		if j.Kind == "transfer" && service.Terminal(state) {
			path := "local"
			if j.FromShard != j.ToShard {
				path = "saga"
			}
			m.Completion.WithLabelValues(path, state).Observe(time.Since(j.Created).Seconds())
			m.Transfers.WithLabelValues(path, state).Inc()
		}
	}
	pools := append([]*pgxpool.Pool{s.Coord}, s.Shards...)
	for i, p := range pools {
		label := "coordinator"
		if i > 0 {
			label = fmt.Sprintf("shard-%d", i-1)
		}
		for _, state := range []string{"total", "idle", "acquired"} {
			state := state
			m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "ledger_database_pool_connections", ConstLabels: prometheus.Labels{"database": label, "state": state}}, func() float64 {
				stat := p.Stat()
				switch state {
				case "idle":
					return float64(stat.IdleConns())
				case "acquired":
					return float64(stat.AcquiredConns())
				default:
					return float64(stat.TotalConns())
				}
			}))
		}
		m.Registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "ledger_database_pool_acquire_seconds_total", ConstLabels: prometheus.Labels{"database": label}}, func() float64 { return p.Stat().AcquireDuration().Seconds() }))
	}
}
