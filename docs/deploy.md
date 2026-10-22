# Deployment guide

## Prerequisites

**To run everything:** install and start [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/).
The Compose plugin is included. PostgreSQL, Kafka, Prometheus, Grafana, Toxiproxy,
k6, and the Go build/test tools run in containers — no separate installs required.
Allocate roughly 6 CPU cores, 8 GB RAM, and 15 GB free disk to Docker.

**Optional local development:** install [Go 1.25+](https://go.dev/doc/install) with
`brew install go`. The smoke/chaos helper scripts use Python 3 (`brew install python`
if absent). No Python packages are required.

---

## Start the default stack

From the repository root:

```sh
cp .env.example .env
docker compose --env-file .env -f deploy/docker-compose.yml up -d --build
python3 scripts/smoke.py
python3 scripts/verify.py
```

Default credentials are in `.env.example`. If you change `API_KEY`, export it for
Python scripts as well. All exposed ports bind to localhost; databases and Kafka
have no host ports.

| Service | Address |
|---|---|
| API | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin / local-grafana-password) |
| Prometheus | http://localhost:9092 |
| Reconciler metrics | http://localhost:9091/metrics |
| Toxiproxy admin | http://localhost:8474 |

Open the provisioned **Distributed Payments Ledger** dashboard in Grafana.

```sh
# Follow logs
docker compose -f deploy/docker-compose.yml logs -f api worker relay

# Stop and preserve data volumes
docker compose -f deploy/docker-compose.yml down
```

---

## API reference

Every endpoint except `/healthz` and `/readyz` requires the `X-API-Key` header.
Every POST requires `Idempotency-Key` (up to 200 bytes). Amounts are integer minor
units (100 = one dollar in a cents-based currency).

### Create an account

```sh
curl http://localhost:8080/accounts \
  -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: create-alice' \
  -H 'Content-Type: application/json' \
  -d '{"initial_deposit":10000}'
```

### Transfer funds

```sh
curl http://localhost:8080/transfers \
  -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: transfer-001' \
  -H 'Content-Type: application/json' \
  -d '{"from_account":"<sender UUID>","to_account":"<receiver UUID>","amount":2500}'
```

### Endpoint table

| Method | Path | Behavior |
|---|---|---|
| POST | /accounts | Create; optional initial deposit balanced against funding account |
| GET | /accounts/{id} | Customer balance, version, metadata |
| GET | /accounts/{id}/entries?cursor=0 | Up to 100 entries; continue after `next_cursor` until empty |
| POST | /transfers | 201 when immediately completed; 202 when queued (cross-shard) |
| GET | /transfers/{id} | Current transfer status and shard routing |
| GET | /operations/{id} | Current account-creation or transfer status |
| GET | /healthz | Coordinator reachability |
| GET | /readyz | Coordinator and all shards reachable |

### Idempotency behaviour

- Account creation returns `id` (account) and `operation_id`. A 202 means queued — poll the operation before spending.
- Transfer retries replay the original status and body even after completion.
- Reusing a key with different fields gives 422; concurrent submissions give 409.
- A 503 has an uncertain outcome: retry with the same key.
- API unknown fields, non-integer amounts, oversized requests, self-transfers, and access to system funding/clearing accounts are rejected.

---

## Tests

```sh
# Full suite with race detector, including real PostgreSQL integration:
docker compose -f deploy/docker-compose.yml run --rm test

# Local Go toolchain (unit tests only):
go test -race ./...

# Include integration tests locally:
TEST_DATABASE_URL='postgres://user:password@localhost:5432/ledger?sslmode=disable' go test -race ./...
```

Integration tests create and drop isolated schemas; the database user must have
`CREATE SCHEMA` permission. Without `TEST_DATABASE_URL`, integration tests skip.

**Coverage includes:**

- Same-shard and cross-shard movements, duplicate step execution, lost debit acknowledgements, multiple worker replicas
- 100 concurrent debits against a balance of 50 — exactly 50 succeed
- Deterministic random transfer sequences with balance/entry and zero-sum audits
- Credit/cancellation races and a committed credit discovered during cancellation
- Account creation recovered after its shard commit; integer overflow safely reversed; intentional cached-balance corruption detected by reconciliation
- Idempotency concurrency, route/body conflict handling, and byte-identical replay
- Lost Kafka publish acknowledgement simulated at the relay boundary, followed by a duplicate publication and one audit receipt

---

## Load generation

```sh
docker compose -f deploy/docker-compose.yml run --rm -e RATE=20 -e DURATION=60s k6
```

The load generator targets `RATE` transfers/second for `DURATION`, uses approximately
80% same-shard / 20% cross-shard traffic, replays each accepted POST three times,
polls terminal outcomes, and reports latency and completion time.

---

## Chaos testing

Run load in one terminal, then inject a fault from another:

```sh
# Terminal 1 — run load
docker compose -f deploy/docker-compose.yml run --rm -e RATE=20 -e DURATION=60s k6

# Terminal 2 — inject one fault at a time
python3 chaos/run.py worker    --seconds 20   # kill the worker for 20 s
python3 chaos/run.py shard     --seconds 20   # stop a shard database
python3 chaos/run.py latency   --seconds 20   # add 500 ms + 5% TCP resets
python3 chaos/run.py partition --seconds 20   # cut the network to target shard
python3 chaos/run.py kafka     --seconds 60   # stop Kafka for 60 s

# After recovery — verify correctness
python3 scripts/verify.py
```

Each fault has a cleanup handler. Run scenarios separately and retain results.
A persistent shard outage intentionally keeps sagas waiting (safety over bounded refund time).

For a deterministic automated suite that restores faults and writes `results/chaos.json`:

```sh
python3 scripts/chaos_suite.py
```

---

## Shard scaling experiments

Stop the default stack, generate an isolated Compose file for 1, 2, or 4 shards,
then run against it on fresh volumes:

```sh
python3 scripts/topology.py 4
docker compose -f deploy/generated-4.json up -d
docker compose -f deploy/generated-4.json run --rm -e RATE=20 -e DURATION=60s k6
python3 scripts/verify.py
docker compose -f deploy/generated-4.json down
```

**Important:** never change shard count or order against existing account data.
Use isolated new projects (separate generated Compose files) for each experiment.
Increase arrival rate gradually and record throughput, latency, Docker allocation, and hardware alongside any scaling claims.

For a short automated 1/2/4 comparison (stops then restores the default stack):

```sh
python3 scripts/benchmark.py --seconds 20 --rate 10
```

Raw summaries, hardware metadata, and logs are written to `results/` (git-ignored).
See [benchmark notes](benchmarks.md) for what was measured and its limitations.

---

## Environment variables

See `.env.example` for all configuration. Key variables:

| Variable | Default | Description |
|---|---|---|
| `API_KEY` | `local-development-key` | Shared API authentication key |
| `COORDINATOR_URL` | (set by Compose) | PostgreSQL DSN for coordinator database |
| `SHARD_URLS` | (set by Compose) | Comma-separated PostgreSQL DSNs for shard databases |
| `KAFKA_BROKERS` | (set by Compose) | Comma-separated Kafka broker addresses |
| `MAX_ATTEMPTS` | `8` | Credit retry budget before transitioning to CANCELLING |

---

## Known limitations

- Single currency; no FX, external banking, per-user authorization, account closure, or online shard rebalancing
- Bootstrap deposits are an administrative demo capability behind the shared API key
- Coordinator durability and Kafka replication are single-node demo configurations
- Static shard order and count must never change against existing accounts
- Pinning container image digests, rotating credentials, TLS, backups, and high availability are future production work
