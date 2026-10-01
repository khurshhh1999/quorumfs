# QuorumFS implementation specification

Status: Q0 bootstrap and Q1 local storage are implemented and locally verified.
The server opens the durable storage engine; its distributed data RPCs remain
UNIMPLEMENTED. Offline local commands support put/get/verify/checkpoint/restore.
Ring, vectors, quorum coordination, deletion and repair below remain proposed.
See `docs/adr/0003-durable-local-storage.md` and `docs/evidence/Q1/README.md` for
implemented storage semantics and evidence. No distributed durability or
performance result is claimed.

## Architecture and scope

Java, gRPC/protobuf, RocksDB JNI and Docker; pin compatible supported versions in Q0. Five nodes, fixed cluster membership for v1, no leader on the data path. Each node can coordinate a request and also store replicas. An operator-distributed versioned membership file establishes the ring; automated membership changes are outside v1 because safe reconfiguration needs a separate protocol.

```text
modules/protocol/        public and internal protobuf contracts
modules/hash-ring/       deterministic virtual-node ring and replica selection
modules/versioning/      version-vector ordering, siblings and resolution
modules/storage/         RocksDB schema, WAL, manifests, chunks and recovery
modules/coordinator/     client requests, strict quorums and deadlines
modules/replication/     replica RPCs, hinted handoff, repair and Merkle exchange
modules/server/          configuration, TLS/auth, health and lifecycle
modules/client/          CLI and Java client with context handling
infra/compose/           five distinct nodes/volumes and network fault proxy
Tests: unit alongside modules; tests/system/ for histories and fault harness.
```

## Ring and quorum rules

Hash namespace plus key with a specified stable 128-bit hash derived from SHA-256. Use 128 virtual nodes per physical node initially. Replica selection walks clockwise and chooses distinct physical nodes; ties have deterministic ordering. Persist node IDs, virtual-node mapping, cluster ID and epoch; never regenerate IDs after restart.

Default cluster size five, replication factor N=3, read R=2, write W=2. Require 1<=R,W<=N<=5 and R+W>N for the supported overlap mode. Majority W is default; overlap is not a claim of linearizability or single-version serialization. Reject invalid configuration. Return errors when enough canonical replicas cannot respond; never silently weaken R/W.

Strict quorum uses canonical ring owners only. Hints and non-owner temporary copies never count toward W. An epoch mismatch fails closed and prompts operator repair; do not merge divergent rings. No automatic resizing/rebalancing in v1. Node replacement preserves identity only after verified recovery or follows an explicit offline reseed procedure.

## Client and replica contracts

Public RPCs: `PutObject` (client-streaming header/chunks), `GetObject` (streaming object or conflict metadata), `HeadObject` (versions and metadata), `DeleteObject`, `ResolveObject` (explicit merged context), `GetClusterInfo`. Internal RPCs: `ReplicateVersion`, `ReadVersions`, `FetchVersion`, `ApplyTombstone`, `ExchangeMerkle`, `FetchRangeVersions`, `DeliverHint`. Authenticate public namespace access and use mTLS between nodes. Reserve protobuf field numbers and maintain compatibility checks.

Put header: namespace, key, request ID, causal context vector, total size, content checksum. Chunks: offset, bytes, checksum. Defaults: 256 KiB chunks, 64 MiB maximum object, 1 KiB key limit, bounded in-flight streams and buffers. Reject gaps, overlaps, duplicates with inconsistent bytes, size mismatch and invalid checksums. Support cancellation/deadlines; interrupted uploads are invisible and cleaned after a configured staging TTL. No unbounded whole-object memory buffering.

Return committed version/vector and digest only after W canonical replicas report complete durable versions. Timeout returns `DEADLINE_EXCEEDED` with outcome unknown: partial commits may exist, so clients must inspect state and use causal context before retrying. Request ID identifies a logical attempt and supports tracing/replay detection; it is not a globally serialized exactly-once guarantee across arbitrary coordinators. Document that blind retries can create siblings.

Get collects complete version metadata from R canonical replicas, merges causal versions, and fetches verified bytes from a holder. If multiple nondominated live versions remain, return explicit conflict metadata with version IDs; client fetches a selected sibling or resolves them. A read cannot return NOT_FOUND based on one reachable node when R is unavailable. A mix of deletion and concurrent live versions is a conflict, not silent deletion. Status codes distinguish invalid request, unavailable quorum, unknown outcome, conflict, checksum corruption and capacity exhaustion.

## Storage and durability

Use RocksDB column families for manifests/version metadata, chunks, vectors/counters, request metadata, hints, tombstones and ring metadata. Encode keys with length prefixes and versioned formats. Stage immutable chunks under upload ID, then expose an object-version manifest only once all bytes and checksums validate. The final manifest and metadata use an atomic WriteBatch.

Keep WAL enabled and use synchronous durable writes for acknowledged replica completion. Staged chunks must be durably flushed/synced before a manifest becomes visible; a manifest cannot point to volatile chunks. Persist coordinator counter allocation durably before reusing the node's causal identity. Close all native RocksDB resources deterministically, bound block cache/write buffers, monitor compaction stalls and disk capacity. Disk-full or sync errors cannot produce successful replica acknowledgments.

Recovery scans unfinished staging uploads and manifests; invisible orphan chunks may be removed after grace period, committed manifests must never be removed as staging. Corrupt chunks are quarantined and repaired from a verified replica; never return unchecked bytes. Backup/checkpoint procedures must preserve manifests, chunks, vectors, tombstones, node identity and epoch together. Verify restore into a fresh volume.

RocksDB synchronous WAL writes are required for the intended crash model; see [RocksDB basic operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations) and [WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29). Durability still depends on storage/fsync behavior. Process-kill tests are not evidence of surviving disk destruction or all hardware failures.

## Version vectors and delete semantics

A vector maps stable node IDs to monotonic counters. Coordinator merges supplied causal context, increments its persisted counter and creates an immutable version. Equal vectors with different payload digests are an integrity error. Vector A dominates B only if every component is >= and at least one is >; incomparable vectors are siblings. No wall-clock last-write-wins. A context-free overwrite can create siblings even when the client intended replacement.

Resolution reads all siblings, merges their context, and writes chosen data as a dominating version. Race with a new write can produce new siblings; report it honestly. Bound sibling count (default 32) and context size; return resource exhaustion when resolution is needed rather than dropping siblings. Fixed membership bounds vector width; do not prune causal entries casually.

Delete writes a versioned tombstone through W canonical replicas. It dominates only the context observed by the caller; a concurrent live version remains visible as a conflict. v1 retains tombstones indefinitely, exposing storage growth as a limitation. Never use time-only tombstone deletion: an old replica or hint could resurrect deleted objects. Tombstone compaction requires a future proven cluster-wide safe watermark and stale-node exclusion protocol.

## Hints, repair and anti-entropy

When a canonical replica is unavailable but W succeeds, persist a hint containing destination, version identity/vector, checksum and tombstone flag, with durable access to the bytes. A hint may reference local data only if GC pins that data until hint acknowledgment. Retry with bounded backoff/rate and remove only after destination durable acknowledgment. Exhausted hint budget blocks further degraded writes before acceptance when recovery promises cannot be honored; expose precise error and alert. Coordinator failure can lose non-quorum hints, so anti-entropy is also required.

Read repair sends merged nondominated versions to stale replicas asynchronously with bounds and durable retry. Never overwrite a concurrent sibling just because it arrived later. Merkle trees cover deterministic token ranges and canonical sorted version metadata, including vector, checksum, size and tombstone. Compare only identical cluster epochs and range boundaries. Use a RocksDB snapshot for each tree generation; concurrent mutations require another pass, not a false declaration of convergence. Compare roots, descend mismatched ranges, stream missing versions, validate checksums and apply idempotently. Run on a configurable interval with byte/CPU budgets; expose lag.

## Security and operations

Authenticate clients, authorize namespace operations, encrypt transport and redact payloads/keys in logs. Enforce request/stream/resource limits and rate-limit repair separately from foreground traffic. Bind administrative endpoints to an internal interface. Logs carry request ID, node ID and epoch; metrics avoid arbitrary-key labels.

Expose quorum success/failure, replica latency, sibling counts, checksum failures, hint backlog/age, repair bytes/lag, Merkle divergence, RocksDB WAL/compaction metrics, disk usage and active streams. Readiness verifies local storage and ring validity; quorum health is reported separately so a network partition does not create restart loops. Runbooks: failed node, partition, disk full, corruption, hint saturation, restore, rolling compatible upgrade and epoch mismatch.

## Fault model and evidence contract

Required campaign: at least 10,000 seeded operations across five nodes with PUT/GET/DELETE, causal overwrites, concurrent writes, conflicts and explicit resolution. Minimum mix: 40% PUT, 30% GET, 15% DELETE, 15% resolution/causal update; retain the actual distribution, payload-size distribution and fault schedule. Track acknowledged writes/deletes separately; 10,000 total operations is not 10,000 writes. Use a durable harness history outside the nodes under test.

Supported initial fault model: process crashes/restarts retaining volumes, temporary network partitions, latency/loss, one canonical replica unavailable per N=3/W=2 operation, and detected corruption recoverable from an intact replica. When quorum is unavailable the required result is failure, not availability. Simultaneous destruction of enough acknowledged copies is outside the durability claim.

An acknowledged version is not lost if it remains retrievable as a live sibling or is causally superseded by a recorded valid write/delete. A version disappearing due only to wall-clock ordering or premature tombstone GC is loss. After healing, wait for a defined repair deadline (initial test budget 120 seconds for the small fixture), then query all canonical replicas directly and compare merged vectors/content checksums with the history oracle. Record late convergence as failure, not an indefinite wait. Unacknowledged timed-out operations may exist and must not be falsely classified as corruption.

Report operations attempted/succeeded/timed out, acknowledged writes, lost acknowledged versions, unavailable reads, conflicts, checksum errors, convergence time, seed, fault timeline, git SHA, container/JDK/RocksDB versions, N/R/W, ring, resource/disk environment and raw histories. Zero observed acknowledged-write loss in this bounded campaign is an empirical result, not a universal durability guarantee.
