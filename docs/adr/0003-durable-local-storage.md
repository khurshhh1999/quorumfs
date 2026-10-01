# ADR 0003: Durable local immutable versions

Status: implemented for Phase 2 / Q1. This is a storage engine, not a distributed
quorum service. Public/internal object RPCs remain UNIMPLEMENTED until their
coordination semantics are available.

## Publication and failures

Allocate a local monotonically increasing sequence with synchronous WAL enabled
before creating an upload. A cancelled/failed upload leaves a gap; never reuse its
sequence. The sequence is not a version vector. Each upload receives a UUID and
an invisible staging record. Store chunks with WAL enabled, then sync the WAL
before synchronously writing the manifest, committed-version index, and staging
removal in one WriteBatch. Only then return local success.

Each chunk is at most 256 KiB. Except for the final chunk it must be exactly
256 KiB. Enforce contiguous offsets, declared size <=64 MiB, 32-byte SHA-256
chunk/object digests, namespace/key limits, and an eight-upload default budget.
No in-memory whole-object buffer. Caller arrays are copied before storage and
hashing. Streams support close/abort, interruption and a 15-minute default TTL.
An abandoned upload expires on a subsequent begin/checkpoint or its own append/
commit. Recovery immediately collects all remaining staging records because the
exclusive DB lock establishes that prior upload sessions cannot still be live.

An I/O failure makes the instance fail closed until reopened. A failed commit
may already be durable; callers retain its UUID and inspect it after reopening.
Never garbage-collect on a failed instance. Recovery refuses to delete any stage
that overlaps a committed index or manifest. Startup validates both directions
of the commit index, the counter floor, and every committed object's chunks and
whole-object digest. Corruption quarantines the volume by rejecting startup;
there is no silent deletion or automatic replica repair in Q1.

Reads preflight the complete object without emitting bytes, then stream with
per-chunk verification again. A caller's failing output stream does not poison
the database. The CLI writes to a temporary file and publishes only a verified
complete output. Operations are serialized per store instance for lifecycle
safety. No throughput or parallel-read performance claim is made.

## On-disk format

Column families: default (legacy identity and format marker), manifests, chunks,
counters, staging, vectors, requests (committed-version index), hints, tombstones,
and ring. Reserved future families contain no invented causal/replication state.

Format v1 keys use a one-byte encoding version, explicit lengths for namespace
and binary key, and UUID bits. Chunk keys append a big-endian nonnegative offset
to the versioned UUID. Manifests contain encoding version, sequence, total size,
chunk size and raw SHA-256. Chunk values contain their checksum followed by data.
Native resources close deterministically; caches and write buffers have shared
8 MiB/16 MiB budgets, with bounded per-family buffers and 128 open files.

The existing Q0 default-column-family identity survives additive family creation.
Q0 binaries cannot reopen a Q1 database with additional families: rollback needs
a pre-upgrade backup, never a blind binary downgrade.

## Recovery and evidence

Checkpoints cover all families, identity, sequence counter and WAL. Require no
active uploads and audit content before checkpointing. Restore copies into a
fresh temporary directory, opens/audits it with the expected identity, then
atomically publishes the destination. A mismatched/corrupt restore does not
replace an existing directory. Checkpoints on the same filesystem may share SST
hardlinks; copy them to independent storage for device-loss protection.

Real process kills exercise staged data, WAL sync, pre-manifest, post-manifest
and acknowledged states. A real 8 MiB Linux tmpfs induces native ENOSPC.
Deterministic hooks cover persistence/sync failure propagation; they are not
hardware power-loss or physical fsync-failure tests. A separate 24 MiB-heap JVM
streams a 64 MiB object. See local verification notes in `docs/evidence/Q1/README.md`.

References: [RocksDB WAL and synchronous writes](https://github.com/facebook/rocksdb/wiki/Basic-Operations),
[RocksDB checkpoints](https://github.com/facebook/rocksdb/wiki/Checkpoints).
