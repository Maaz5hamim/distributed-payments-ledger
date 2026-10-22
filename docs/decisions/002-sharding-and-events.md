# ADR 002: Static rendezvous routing and transactional outbox

Rendezvous hashing minimizes ownership moves when a shard is added. This demo
requires a fixed ordered map; ownership movement requires an explicit offline
migration. More shards do not remove contention on a hot clearing account or the
single coordinator, so near-linear throughput scaling must be measured, not assumed.

Use an outbox in the ledger database and an audit consumer with a unique event ID.
Acknowledgements cannot atomically span PostgreSQL and Kafka, so transport remains
at least once. Stable IDs and transactional consumer effects provide deduplication.
The sample consumer records audit receipts only.
