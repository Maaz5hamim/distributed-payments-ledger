# Measured validation — October 22, 2026

These are short local functional runs, not peak-throughput or production claims.
Hardware: Apple M5, 24 GiB host memory; Docker Desktop allocated 10 virtual CPUs
and 8,319,504,384 bytes of memory (about 7.75 GiB). Docker Engine 29.7.2,
Linux/aarch64 containers. One API, one worker, one relay, and one consumer per run.

## Fixed-rate 1/2/4 comparison

Command: `python3 scripts/benchmark.py --seconds 20 --rate 10`.
Each topology used isolated persistent volumes. Forty customers were seeded with
balanced bootstrap deposits. The generator targeted 10 transfers/second for 20
seconds, chose approximately 80% same-shard / 20% cross-shard traffic at random
(100% same-shard with one shard), and replayed each accepted POST three times.

| Shards | Transfers | Observed transfers/s | Local API p50 / p99 ms | Saga acceptance p50 / p99 ms | Client-observed completion p99 ms | Transfer failures |
|---|---|---|---|---|---|---|
| 1 | 201 | 9.98 | 8.60 / 13.06 | — | 20 | 0 |
| 2 | 201 | 9.98 | 8.62 / 15.94 | 3.28 / 5.86 | 212 | 0 |
| 4 | 201 | 9.97 | 7.99 / 13.95 | 3.52 / 6.12 | 215 | 0 |

All runs passed every retry-response/completion check, had no interrupted
iterations, and subsequently passed fresh reconciliation with zero mismatches,
all jobs terminal, and all outboxes drained. The event checker confirmed all 201
terminal transfer IDs per topology were recorded once by the audit consumer
through Kafka. These runs did not saturate the system and therefore demonstrate
neither maximum capacity nor a scaling speedup. Compare latency distributions
under longer, repeated runs and increase arrival rates before making scaling
claims. The initial standalone two-shard smoke load also passed 201 transfers.

API latency measures the first POST only. Saga acceptance is asynchronous and
therefore cannot be compared to local execution as though both were completed
transfers. Client completion includes three replay requests and 100 ms polling;
it is not a precise measurement of the worker's internal execution time.
The worker also exports `ledger_saga_completion_seconds` for operational timings.

## PostgreSQL correctness tests

The real-PostgreSQL integration suite and race detector passed. Coverage includes:

- Same-shard and cross-shard movements, duplicate step execution, lost debit
  acknowledgements, and multiple workers claiming queued work.
- One hundred concurrent debits against a balance of 50: exactly 50 succeed.
- Deterministic random transfer sequences with balance/entry and zero-sum audits.
- Credit/cancellation races and a committed credit discovered during cancellation.
- Account creation recovered after its shard commit; integer overflow safely
  reversed; intentional cached-balance corruption detected by reconciliation.
- Idempotency concurrency, route/body conflict handling, and byte-identical replay.
- Lost Kafka publish acknowledgement simulated at the relay boundary, followed
  by a duplicate publication and one audit receipt.

Integration tests use isolated schemas and explicitly skip without a configured
PostgreSQL test database. CI configures that database. The ordinary container
build compiles and runs unit tests; it does not itself run database integration.

## Live recovery checks

`python3 scripts/chaos_suite.py` ran continuous local transfers on shard 0,
replayed every test transfer three times, and exercised these default-stack faults:

| Fault | Injection | Observed result |
|---|---|---|
| Worker death | Immediate stop for 5 seconds | Pending saga completed after restart |
| Target shard outage | Immediate stop for 5 seconds | Transfer completed after recovery |
| Kafka outage | Immediate stop for 5 seconds | Ledger transfers completed; event delivery recovered |
| Target latency | 500 ms per downstream round trip for 5 seconds | Transfer completed after latency removal |
| Target partition | Proxy disabled until credit retry budget exhausted | Remained CANCELLING while unreachable, then REVERSED after a durable credit fence; receiver unchanged |

After restoring all faults, verification reported zero mismatches, all jobs
terminal, and empty outboxes. A separate real Kafka check confirmed all 945
terminal transfer IDs accumulated in the default stack were present once in the
audit consumer (this includes earlier smoke/load attempts, not just the final
fault suite). Outbox batch settings were adjusted after validation exposed a
throughput bottleneck; the short scaling comparisons use that corrected relay.

The latency utility additionally supports 5% TCP resets, but that combination was
not included in the recorded live suite. A 60-second Kafka outage, repeated
worker kills every 30 seconds, coordinator failures, and sustained saturation
runs remain extended experiments. No claim is made that every possible failure
schedule has been tested. Network partitions deliberately delay refunds until
an uncertain credit can be resolved safely.

Raw summaries, hardware metadata, and recovery evidence are in local `results/`
(ignored by Git). Re-run the scripts to reproduce the experiment; retain those
artifacts with any performance claims.
