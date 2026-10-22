package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"distributed-payments-ledger/internal/ledger"
	"distributed-payments-ledger/internal/observability"
	"distributed-payments-ledger/internal/service"
	"distributed-payments-ledger/internal/shard"
	"github.com/jackc/pgx/v5"
)

type API struct {
	Service *service.Service
	Key     string
	Metrics *observability.Metrics
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, s string) {
	reply(w, status, map[string]string{"error": s})
}
func (a *API) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if e := a.Service.Coord.Ping(r.Context()); e != nil {
			fail(w, 503, "coordinator unavailable")
			return
		}
		reply(w, 200, map[string]string{"status": "ok"})
	})
	m.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if e := a.Service.Coord.Ping(r.Context()); e != nil {
			fail(w, 503, "coordinator unavailable")
			return
		}
		for _, p := range a.Service.Shards {
			if e := p.Ping(r.Context()); e != nil {
				fail(w, 503, "shard unavailable")
				return
			}
		}
		reply(w, 200, map[string]string{"status": "ready"})
	})
	m.HandleFunc("POST /accounts", a.write)
	m.HandleFunc("POST /transfers", a.write)
	m.HandleFunc("GET /accounts/{id}", a.read)
	m.HandleFunc("GET /accounts/{id}/entries", a.read)
	m.HandleFunc("GET /transfers/{id}", a.read)
	m.HandleFunc("GET /operations/{id}", a.read)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-API-Key")), []byte(a.Key)) != 1 {
			fail(w, 401, "invalid API key")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		rec := &recorder{ResponseWriter: w, status: 200}
		m.ServeHTTP(rec, r.WithContext(ctx))
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		a.Metrics.Requests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
	})
}

type recorder struct {
	http.ResponseWriter
	status int
}

func (w *recorder) WriteHeader(s int) { w.status = s; w.ResponseWriter.WriteHeader(s) }
func (a *API) write(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if len(key) == 0 || len(key) > 200 {
		fail(w, 400, "Idempotency-Key required (maximum 200 bytes)")
		return
	}
	var in *struct {
		From    string `json:"from_account"`
		To      string `json:"to_account"`
		Amount  int64  `json:"amount"`
		Deposit int64  `json:"initial_deposit"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	dec.DisallowUnknownFields()
	if e := dec.Decode(&in); e != nil || in == nil {
		fail(w, 400, "invalid JSON")
		return
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		fail(w, 400, "expected one JSON object")
		return
	}
	kind := "transfer"
	if r.URL.Path == "/accounts" {
		kind = "account"
		if in.Deposit < 0 || in.Amount != 0 || in.From != "" || in.To != "" {
			fail(w, 400, "invalid account request")
			return
		}
	} else if !customerID(in.From) || !customerID(in.To) || in.From == in.To || in.Amount <= 0 || in.Deposit != 0 {
		fail(w, 400, "invalid transfer request")
		return
	}
	canonical, _ := json.Marshal(in)
	sum := sha256.Sum256(append([]byte(r.URL.Path+":"), canonical...))
	j := service.Job{ID: ledger.ID(), Kind: kind, From: in.From, To: in.To, Amount: in.Amount}
	if kind == "account" {
		j.To = ledger.ID()
		j.Amount = in.Deposit
	}
	j.ToShard = shard.Route(j.To, len(a.Service.Shards))
	j.FromShard = j.ToShard
	if kind == "transfer" {
		j.FromShard = shard.Route(j.From, len(a.Service.Shards))
	}
	started := time.Now()
	path := "local"
	if j.FromShard != j.ToShard {
		path = "saga"
	}
	j, e := a.Service.Submit(r.Context(), key, hex.EncodeToString(sum[:]), j)
	if e != nil {
		switch {
		case errors.Is(e, service.ErrConflict):
			fail(w, 422, e.Error())
		case errors.Is(e, service.ErrBusy):
			fail(w, 409, e.Error())
		default:
			slog.Error("submit", "error", e)
			fail(w, 503, "coordinator unavailable; retry with same key")
		}
		return
	}
	// Cross-shard work is asynchronous. Account and local operations attempt the
	// fast path; a durable intent remains recoverable if this process dies.
	if j.Kind == "account" || j.FromShard == j.ToShard {
		if _, e = a.Service.Process(r.Context(), j.ID); e != nil {
			slog.Warn("fast path", "error", e)
		}
	}
	status, data, e := a.Service.Response(r.Context(), j.ID)
	if e != nil {
		fail(w, 503, "response unavailable; retry with same key")
		return
	}
	a.Metrics.Duration.WithLabelValues(path, strconv.Itoa(status)).Observe(time.Since(started).Seconds())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(data)
}
func customerID(id string) bool {
	return ledger.ValidID(id) && id != ledger.Funding && id != ledger.Clearing
}
func (a *API) read(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !ledger.ValidID(id) {
		fail(w, 400, "invalid ID")
		return
	}
	if strings.HasPrefix(r.URL.Path, "/transfers/") || strings.HasPrefix(r.URL.Path, "/operations/") {
		j, e := a.Service.Get(r.Context(), id)
		if errors.Is(e, pgx.ErrNoRows) {
			fail(w, 404, "not found")
			return
		}
		if e != nil {
			fail(w, 503, "coordinator unavailable")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/transfers/") && j.Kind != "transfer" {
			fail(w, 404, "not found")
			return
		}
		reply(w, 200, j)
		return
	}
	if !customerID(id) {
		fail(w, 404, "not found")
		return
	}
	p := a.Service.Shards[shard.Route(id, len(a.Service.Shards))]
	var data []byte
	var e error
	if strings.HasSuffix(r.URL.Path, "/entries") {
		cursor := int64(0)
		if s := r.URL.Query().Get("cursor"); s != "" {
			cursor, e = strconv.ParseInt(s, 10, 64)
			if e != nil || cursor < 0 {
				fail(w, 400, "invalid cursor")
				return
			}
		}
		var exists bool
		e = p.QueryRow(r.Context(), "SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1 AND kind='customer')", id).Scan(&exists)
		if e == nil && !exists {
			fail(w, 404, "not found")
			return
		}
		if e == nil {
			e = p.QueryRow(r.Context(), `SELECT json_build_object('entries',COALESCE(json_agg(t ORDER BY t.id),'[]'::json),'next_cursor',max(t.id)) FROM (SELECT id,transfer_id,step,account_id,amount,created_at FROM entries WHERE account_id=$1 AND id>$2 ORDER BY id LIMIT 100) t`, id, cursor).Scan(&data)
		}
	} else {
		e = p.QueryRow(r.Context(), "SELECT row_to_json(a) FROM accounts a WHERE id=$1 AND kind='customer'", id).Scan(&data)
	}
	if errors.Is(e, pgx.ErrNoRows) {
		fail(w, 404, "not found")
		return
	}
	if e != nil {
		fail(w, 503, "shard unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
