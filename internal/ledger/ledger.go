package ledger

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Funding = "00000000-0000-0000-0000-000000000001"
const Clearing = "00000000-0000-0000-0000-000000000002"

var ErrFunds = errors.New("insufficient funds")
var ErrAccount = errors.New("customer account not found")
var ErrOverflow = errors.New("balance exceeds integer range")
var ErrCancelled = errors.New("step cancelled")

func Permanent(err error) bool {
	return errors.Is(err, ErrFunds) || errors.Is(err, ErrAccount) || errors.Is(err, ErrOverflow) || errors.Is(err, ErrCancelled)
}
func ID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func ValidID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func lock(ctx context.Context, tx pgx.Tx, id string) error {
	_, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", id)
	return e
}
func applied(ctx context.Context, tx pgx.Tx, id, step string) (bool, error) {
	var s string
	e := tx.QueryRow(ctx, "SELECT status FROM steps WHERE transfer_id=$1 AND step=$2", id, step).Scan(&s)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if s == "CANCELLED" {
		return false, ErrCancelled
	}
	return true, nil
}
func mark(ctx context.Context, tx pgx.Tx, id, step string) error {
	_, e := tx.Exec(ctx, "INSERT INTO steps(transfer_id,step,status) VALUES($1,$2,'APPLIED')", id, step)
	return e
}
func move(ctx context.Context, tx pgx.Tx, id, step, from, to string, amount int64) error {
	if amount <= 0 || from == to {
		return errors.New("invalid movement")
	}
	rows, e := tx.Query(ctx, "SELECT id::text,kind,balance FROM accounts WHERE id=$1 OR id=$2 ORDER BY id FOR UPDATE", from, to)
	if e != nil {
		return e
	}
	balances := map[string]int64{}
	kinds := map[string]string{}
	for rows.Next() {
		var id, k string
		var b int64
		if e = rows.Scan(&id, &k, &b); e != nil {
			rows.Close()
			return e
		}
		balances[id] = b
		kinds[id] = k
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if len(balances) != 2 {
		return ErrAccount
	}
	if kinds[from] == "customer" && balances[from] < amount {
		return ErrFunds
	}
	if balances[to] > math.MaxInt64-amount || balances[from] < math.MinInt64+amount {
		return ErrOverflow
	}
	if e = mark(ctx, tx, id, step); e != nil {
		return e
	}
	for _, leg := range []struct {
		id string
		a  int64
	}{{from, -amount}, {to, amount}} {
		if _, e = tx.Exec(ctx, "INSERT INTO entries(transfer_id,step,account_id,amount) VALUES($1,$2,$3,$4)", id, step, leg.id, leg.a); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "UPDATE accounts SET balance=balance+$2,version=version+1 WHERE id=$1", leg.id, leg.a); e != nil {
			return e
		}
	}
	return nil
}
func Event(ctx context.Context, tx pgx.Tx, id, state, from, to string, amount int64) error {
	payload, _ := json.Marshal(map[string]any{"event_id": id, "transfer_id": id, "type": "Transfer" + state, "state": state, "from_account": from, "to_account": to, "amount": amount})
	_, e := tx.Exec(ctx, "INSERT INTO outbox(event_id,transfer_id,payload) VALUES($1,$1,$2) ON CONFLICT(event_id) DO NOTHING", id, string(payload))
	return e
}
func Create(ctx context.Context, p *pgxpool.Pool, operation, id string, deposit int64) error {
	tx, e := p.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = lock(ctx, tx, operation); e != nil {
		return e
	}
	ok, e := applied(ctx, tx, operation, "account")
	if e != nil {
		return e
	}
	if ok {
		return nil
	}
	if _, e = tx.Exec(ctx, "INSERT INTO accounts(id,kind) VALUES($1,'customer')", id); e != nil {
		return e
	}
	if deposit > 0 {
		e = move(ctx, tx, operation, "account", Funding, id, deposit)
	} else {
		e = mark(ctx, tx, operation, "account")
	}
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func Apply(ctx context.Context, p *pgxpool.Pool, id, step, from, to string, amount int64) error {
	tx, e := p.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = lock(ctx, tx, id); e != nil {
		return e
	}
	ok, e := applied(ctx, tx, id, step)
	if e != nil {
		return e
	}
	if ok {
		return nil
	}
	// Public customer IDs are checked by the API. Internal clearing/funding IDs
	// enter only through these explicit step definitions.
	var customer string
	switch step {
	case "debit", "local":
		customer = from
	case "credit", "compensate":
		customer = to
	default:
		return errors.New("invalid step")
	}
	var exists bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1 AND kind='customer')", customer).Scan(&exists); e != nil {
		return e
	}
	if !exists {
		return ErrAccount
	}
	if step == "local" {
		if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM accounts WHERE id=$1 AND kind='customer')", to).Scan(&exists); e != nil {
			return e
		}
		if !exists {
			return ErrAccount
		}
	}
	if e = move(ctx, tx, id, step, from, to, amount); e != nil {
		return e
	}
	if step == "local" {
		if e = Event(ctx, tx, id, "COMPLETED", from, to, amount); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

// CancelCredit resolves uncertainty before a refund. If a delayed credit wins
// the lock, completion wins; if cancellation wins, every future credit is fenced.
func CancelCredit(ctx context.Context, p *pgxpool.Pool, id string) (bool, error) {
	tx, e := p.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	if e = lock(ctx, tx, id); e != nil {
		return false, e
	}
	ok, e := applied(ctx, tx, id, "credit")
	if e != nil && !errors.Is(e, ErrCancelled) {
		return false, e
	}
	if ok {
		return true, nil
	}
	if _, e = tx.Exec(ctx, "INSERT INTO steps(transfer_id,step,status) VALUES($1,'credit','CANCELLED') ON CONFLICT DO NOTHING", id); e != nil {
		return false, e
	}
	return false, tx.Commit(ctx)
}
func Emit(ctx context.Context, p *pgxpool.Pool, id, state, from, to string, amount int64) error {
	tx, e := p.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = Event(ctx, tx, id, state, from, to, amount); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

type Audit struct {
	Mismatches    int64
	Sum, Clearing string
	Unsent        int64
	Lag           float64
}

func Reconcile(ctx context.Context, tx pgx.Tx) (Audit, error) {
	var a Audit
	e := tx.QueryRow(ctx, `SELECT count(*) FROM accounts a WHERE a.balance::numeric<>COALESCE((SELECT sum(e.amount) FROM entries e WHERE e.account_id=a.id),0)`).Scan(&a.Mismatches)
	if e != nil {
		return a, e
	}
	e = tx.QueryRow(ctx, `SELECT COALESCE(sum(amount),0)::text FROM entries`).Scan(&a.Sum)
	if e != nil {
		return a, e
	}
	if sum, ok := new(big.Int).SetString(a.Sum, 10); !ok || sum.Sign() != 0 {
		a.Mismatches++
	}
	e = tx.QueryRow(ctx, `SELECT balance::text FROM accounts WHERE id=$1`, Clearing).Scan(&a.Clearing)
	if e != nil {
		return a, e
	}
	e = tx.QueryRow(ctx, `SELECT count(*),COALESCE(EXTRACT(EPOCH FROM now()-min(created_at)),0)::float8 FROM outbox WHERE sent_at IS NULL`).Scan(&a.Unsent, &a.Lag)
	return a, e
}
