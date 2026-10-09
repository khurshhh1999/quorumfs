# Deletion and replica recovery

## Delete and recreate

With the authenticated fixture and `client` variable from the README:

```bash
context=$("$client" context localhost:19002 --insecure demo "$key")
"$client" delete localhost:19001 --insecure demo "$key" "$context"
"$client" head localhost:19003 --insecure demo "$key"
context=$("$client" context localhost:19003 --insecure demo "$key")
"$client" put localhost:19004 --insecure demo "$key" "$work/input.txt" "$context"
```

CLI metadata has five fields: UUID, size, SHA-256, context hex and `live` or
`tombstone`. DELETE without context (or with `-`) is a blind causal operation,
not an unconditional overwrite. It can conflict with an existing live version.
Use HEAD/context to inspect and merge observed siblings. GET without a selected
UUID reports ABORTED when concurrent heads remain; a selected tombstone reports
NOT_FOUND. Deleted data and tombstones remain retained indefinitely, including
historical bytes. Never manually remove tombstones or old database records.

## Recover an offline canonical owner

Restore connectivity and restart the same node with its original identity,
configuration and volume. Surviving coordinators replay pending work without a
foreground read. Every destination must acknowledge the complete durable version
before its intent can disappear. Repeated delivery is safe, including after a
lost acknowledgment or restart. A quorum read also discovers missing maximal
versions and schedules bounded, durable delivery after verified payload capture.

Watch server logs:

- `event=hint_backlog`: pending versions, destination count, pinned bytes and
  oldest age in milliseconds. Persistent growth requires investigation.
- `event=hint_retry`: destination and capped backoff attempt; inspect connectivity,
  matching membership, peer credentials, disk space and destination logs.
- `event=hint_ack`: validated durable destination acknowledgment.
- `event=hint_worker_error`: capture/read/integrity/storage failure; preserve the
  database and inspect local storage and startup audit output.
- `event=read_repair_deferred`: preparation admission or capture failed; later
  reads can rediscover work. This does not weaken the foreground read quorum.

There are 128 pending-version slots and a 256 MiB pinned-payload budget per node.
A full queue rejects new writes with RESOURCE_EXHAUSTED before dispatch, even if
W owners are reachable. Restore the failing destination to drain it. Do not
manually discard hints to force progress. Retries back off to 64 seconds; a full
backlog can take multiple cycles to drain. The queue has no expiry or automatic
capacity expansion. Native WAL/compaction and retained history need additional
free space beyond these logical limits.

Read repair discovery has two preparation slots and no unbounded queue. It can
be deferred before durable capture without failing a valid quorum read. Captured
work survives restart and checkpoint restore. Anti-entropy is not yet implemented:
if hints are lost and an object is never read, divergence may persist. Handoff
also cannot reconstruct a payload when every durable copy is lost.

## Upgrade, checkpoint and rollback

Stop all five nodes and take verified backups before upgrading. This binary
synchronously upgrades storage marker 1 to 2 when opening a database. Previous
binaries reject marker 2. Upgrade all nodes before resuming data service.
Checkpoints include pending hint payloads/destinations as well as live versions,
tombstones and causal counters. Restore only using the existing fresh-target
procedure in [storage and backups](storage.md), retaining matching identity and
membership. Never copy one node's volume to a different identity.

Do not downgrade in place or use a stale single-node backup to originate new
causal writes. A rollback requires a verified whole-cluster recovery point plus
reconciliation of subsequent acknowledged writes. Mixed-version rolling upgrade
is outside the supported preview. No history or hint garbage collection command
is provided.
