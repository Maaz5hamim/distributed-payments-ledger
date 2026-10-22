# Distributed Payments Ledger

A high-reliability bank ledger built in **Go** and **PostgreSQL**. 

This project demonstrates how to build a payment system where accounts live on different database servers (**shards**). It guarantees that **books always balance** and **no money is ever lost**, even if databases crash, workers die, or network connections drop mid-transfer.


## 💡 The Core Problem & Solution

### The Problem

Imagine moving $100 from **Alice** to **Bob**:
* Alice’s account is stored on **Database Shard 0**.
* Bob’s account is stored on **Database Shard 1**.

If you deduct $100 from Alice on Shard 0, and the network crashes before you can add $100 to Bob on Shard 1, $100 vanishes into thin air. 

Traditional solutions lock up both databases simultaneously until both finish (Two-Phase Commit / 2PC). However, if the managing server crashes mid-lock, both databases stay frozen, blocking other users.

### The Solution: Durable Steps + Safety Fences

This project avoids frozen locks by using a **Durable Saga pattern**:

1. **Write down the plan first:** Before touching anyone's money, the API writes a transfer job to a Central Coordinator database.
2. **Step-by-step progress:** A background worker executes the transfer in clear, sequential steps:
   - **Step 1:** Move money from Alice to a temporary "Hold Account" on Shard 0.
   - **Step 2:** Move money from a temporary "Hold Account" to Bob on Shard 1.
3. **Automatic recovery:** If a worker or database crashes at any point, another worker picks up the job, checks where it left off on the plan, and safely resumes or refunds the money.


## 🏗️ System Overview

Here is how the main parts of the system interact when a user requests a money transfer:

```
                  ┌─────────────────────────────────┐
                  │          Client / User          │
                  └────────────────┬────────────────┘
                                   │
                                   │ 1. POST /transfers
                                   ▼
                  ┌─────────────────────────────────┐
                  │           API Server            │
                  │           (cmd/api)             │
                  └────────────────┬────────────────┘
                                   │
                                   │ 2. Save transfer request & plan
                                   ▼
┌───────────────────────────────────────────────────────────────────────┐
│                     Central Coordinator Database                      │
│      Stores overall transfer status, jobs, and idempotency keys       │
└──────────────────────────────────┬────────────────────────────────────┘
                                   │
                                   │ 3. Pick up pending job
                                   ▼
                  ┌─────────────────────────────────┐
                  │         Background Worker       │
                  │           (cmd/worker)          │
                  └───────┬─────────────────┬───────┘
                          │                 │
      4a. Deduct from     │                 │ 4b. Deposit into
          Sender Account  │                 │     Receiver Account
                          ▼                 ▼
             ┌──────────────────┐     ┌──────────────────┐
             │ Database Shard 0 │     │ Database Shard 1 │
             │ (Sender Account) │     │(Receiver Account)│
             └────────┬─────────┘     └────────┬─────────┘
                      │                        │
                      └────────────┬───────────┘
                                   │ 5. Store activity log
                                   ▼
             ┌───────────────────────────────────────────┐
             │            Transactional Outbox           │
             └─────────────────────┬─────────────────────┘
                                   │
                                   │ 6. Publish audit events
                                   ▼
             ┌───────────────────────────────────────────┐
             │                Kafka Bus                  │
             └─────────────────────┬─────────────────────┘
                                   │
                                   │ 7. Process receipt
                                   ▼
                  ┌─────────────────────────────────┐
                  │          Audit Consumer         │
                  │           (cmd/consumer)        │
                  └─────────────────────────────────┘
```


## 🧩 Component Breakdown

| Component | Simple Explanation |
|---|---|
| **API Server** (`cmd/api`) | The entry point for requests. It checks for duplicate requests (Idempotency) and records the intent to transfer money. |
| **Central Coordinator** | A core database that acts as the "master task list." It tracks the status of every ongoing transfer. |
| **Worker** (`cmd/worker`) | The muscle of the system. Background processes pick up tasks from the Coordinator task list and update account balances on individual shards. |
| **Database Shards** | Individual PostgreSQL databases holding customer accounts and ledger entries. Accounts are spread across shards to share the load. |
| **Transactional Outbox & Relay** | A backup message system built into each shard database. It guarantees that event notifications reach Kafka even if network connections fail temporarily. |
| **Kafka & Audit Consumer** | Collects transaction history and receipts from all shards, deduplicates events, and maintains a clean audit trail. |
| **Reconciler** (`cmd/reconciler`) | An automated auditor that periodically calculates total money across all shards to ensure the system math always balances to zero. |


## 🔄 How a Money Transfer Works Step-by-Step

When a transfer moves across two different database shards, it follows a multi-step workflow (a **Saga**):

```
       [ PENDING ]
            │
            │ Step 1: Take money from Sender -> Put in Sender Hold Account
            ▼
       [ DEBITED ]
            │
            ├───► (Normal Flow) ──► Step 2: Move from Receiver Hold -> Receiver
            │                            │
            │                            ▼
            │                      [ COMPLETED ]
            │
            └───► (Timeout or Error) ──► [ CANCELLING ]
                                              │
                              ┌───────────────┴───────────────┐
                              │ Credit was already committed  │ Credit durably fenced
                              ▼                               ▼
                        [ COMPLETING ]               [ COMPENSATING ]
                              │                               │
                              ▼                               │ Step 3: Refund Sender
                        [ COMPLETED ]                         ▼
                                                        [ REVERSING ]
                                                              │
                                                              ▼
                                                        [ REVERSED ]
```

1. **PENDING:** The transfer is logged and waiting to start.
2. **DEBITED:** Money is moved from the sender's account into a temporary hold account on the sender's shard.
3. **COMPLETED:** Money is moved from a hold account on the receiver's shard into the receiver's main balance.
4. **CANCELLING / REVERSED:** If the receiver's shard is unreachable or rejects the transfer, the system double-checks that the target account wasn't credited, blocks future attempts, and safely refunds the sender.


## 🛡️ Key Safety Features

* **Zero-Sum Guarantee:** Money is never created or destroyed. Every credit has an equal debit.
* **Duplicate Protection (Idempotency):** If a user clicks "Pay" three times by mistake, the system processes it only once and returns the cached result for the extra clicks.
* **Crash Proofing:** If any server crashes in the middle of a process, another worker picks up where it left off without double-charging or losing data.
* **Tombstone Fencing:** If a transfer times out, the system leaves a "stop marker" on the destination database to ensure money isn't deposited late after a refund has already been issued.


## 📊 System Benchmarks

Tested on Apple M5 (Docker Desktop, 10 vCPUs, 7.75 GiB RAM). Fixed rate of 10 transfers/s for 20 seconds, ~80% same-shard / ~20% cross-shard traffic. Full notes: [docs/benchmarks.md](docs/benchmarks.md).

| Shards | Transfers | Failures | API p50 / p99 | Client completion p99 |
|---|---|---|---|---|
| **1 Shard** | 201 | 0 | 8.6 ms / 13.1 ms | **20 ms** |
| **2 Shards** | 201 | 0 | 8.6 ms / 15.9 ms | **212 ms** |
| **4 Shards** | 201 | 0 | 8.0 ms / 14.0 ms | **215 ms** |

The jump in client completion time from 1 to 2 shards (20 ms → 212 ms) reflects the cost of cross-shard transfers: those must wait for the background worker to execute multiple saga steps, each requiring a coordinator poll. Same-shard transfers complete inline and return 201 directly. More shards did not change throughput because the system was not saturated at 10 tx/s — these runs prove correctness, not peak capacity.

### Fault Recovery Under Load

| Fault | Outcome |
|---|---|
| **Worker crash** | Pending saga resumed automatically after restart |
| **Target database outage** | Transfer completed once the shard recovered |
| **Kafka outage** | Ledger writes completed; event delivery recovered after broker restart |
| **Network partition** | Stayed CANCELLING until credit outcome confirmed, then REVERSED — receiver balance unchanged |

After all faults: zero balance mismatches, all jobs terminal, all outboxes drained.


## 🚀 Quick Start

### Requirements
* **Docker** & **Docker Compose**
* **Python 3** (for running verification scripts)

### Setup & Run

1. **Copy the environment configuration:**
   ```sh
   cp .env.example .env
   ```

2. **Start all services:**
   ```sh
   docker compose --env-file .env -f deploy/docker-compose.yml up -d --build
   ```

3. **Run sanity checks:**
   ```sh
   python3 scripts/smoke.py
   python3 scripts/verify.py
   ```

### Live Service URLs

| Service | Local Address | What It Does |
|---|---|---|
| **API** | `http://localhost:8080` | Endpoint to send transfers (`POST /transfers`) |
| **Grafana Dashboard** | `http://localhost:3000` | Visual dashboard for monitoring metrics |
| **Prometheus** | `http://localhost:9092` | Metrics collection service |
| **Toxiproxy Admin** | `http://localhost:8474` | Network fault-injection tool (for chaos testing) |


## 📂 Project Structure

```
cmd/            Entry points for each runnable service (API, Worker, Relay, Consumer, Reconciler)
internal/       Core business logic (ledger rules, saga workflow, HTTP endpoints)
db/             Database migration files and SQL schemas
deploy/         Docker Compose and monitoring configurations (Prometheus, Grafana, Toxiproxy)
docs/           Detailed technical design notes and decision documents
scripts/        Smoke tests, verification, benchmark, and chaos suite scripts
chaos/          Fault injection runner (worker kill, shard outage, latency, partition, Kafka)
loadtest/       k6 load generator script
results/        Raw benchmark and recovery artifacts (git-ignored)
```


## 📚 Further Reading

| Document | What's inside |
|---|---|
| [Deployment guide](docs/deploy.md) | Full setup, API reference, test suite, load/chaos instructions |
| [Architecture](docs/architecture.md) | Correctness model, outbox, reconciliation, and known limits |
| [ADR 001 — Sagas & credit fencing](docs/decisions/001-distributed-commit.md) | Why not 2PC; how tombstone fencing prevents double-pay |
| [ADR 002 — Sharding & outbox](docs/decisions/002-sharding-and-events.md) | Rendezvous hashing; at-least-once Kafka with deduplication |
| [Benchmark notes](docs/benchmarks.md) | Full methodology, raw numbers, and chaos recovery evidence |
