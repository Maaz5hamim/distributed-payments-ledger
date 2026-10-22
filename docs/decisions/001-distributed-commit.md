# ADR 001: Durable intents, sagas, and credit fencing

Use a coordinator durable command plus retry-safe balanced local transactions.
This closes the same-shard API/coordinator crash gap without two-phase commit.
A database transaction row lock provides worker ownership for one bounded step.

A credit timeout does not prove failure. Before compensating, acquire the target
step lock and persist a cancellation tombstone. If credit committed first, finish
instead. This adds a CANCELLING phase and can delay refunds through a partition,
but prevents a late credit from racing with a refund.

## Alternatives

Two-phase commit gives a distributed atomic outcome but retains prepared locks
when the coordinator fails. Blind compensation after a retry count is unsafe
because an ambiguous credit may already have committed. Time-based worker leases
alone do not fence a delayed shard write.
