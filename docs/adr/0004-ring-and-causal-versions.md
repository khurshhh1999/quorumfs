# ADR 0004: Fixed ownership and durable causal versions

Status: implemented for Phase 3 / Q2. Distributed coordination remains Q3 work.

## Ownership

HashRing uses the first 128 bits of SHA-256, compared as unsigned big-endian
positions. Hash inputs contain an ASCII domain (`object` or `vnode`), a four-byte
big-endian namespace/node-ID byte length, its UTF-8 bytes, then a four-byte key
length and key bytes. Vnode keys are four-byte big-endian indices 0..127. This
separates namespace/key boundaries and vnode/object domains. Tokens sort by
position, node ID, then vnode index. Ownership starts at the first token >= the
object position, wraps clockwise, and selects N distinct physical nodes.

Exactly five unique fixed members and valid strict N/R/W quorums are required.
Canonical ASCII serialization includes the algorithm version, cluster, positive
epoch, quorums, sorted member IDs/endpoints, and all 640 sorted vnode positions.
Decoding rebuilds the ring and requires byte-for-byte equality. Storage persists
these bytes and the local node ID synchronously before causal writes. Restart
and local replica ingestion reject any configuration mismatch, including drift
within the same epoch. Server startup logs a deterministic 128-bit fingerprint;
compatibility checks compare full canonical bytes, not only the fingerprint.
There is no membership change, automatic rebalance, peer handshake or RPC routing
in this milestone. Endpoints and node identities remain operator supplied.

## Causal ordering

VectorClock is immutable, has at most five positive signed-64-bit components,
and treats absent components as zero. Stored vectors must use ring members and
have at least one component. Canonical serialization sorts node IDs, includes a
version header, and rejects duplicates, noncanonical numbers and trailing data.
Merge is componentwise maximum; comparison yields before/equal/after/concurrent.
No wall-clock timestamp decides which data survives.

A local write merges only the context explicitly supplied by its caller and
allocates a synchronous node counter strictly above both its durable counter
and the supplied local component. Cancelled allocations leave gaps. Replicated
versions preserve the incoming vector and advance the local counter floor.
Counter overflow and unknown identities fail closed. The UUID identifies the
local immutable record; repeated ingestion may retain duplicate history, but
equal vectors/digests have one deterministic head (smallest UUID text). Equal
vectors with different content digests are rejected even after supersession.

The maximal antichain is the visible sibling set. A maximum of 32 siblings is
enforced at publication, without pruning incomparable data. Explicit resolution
merges the contexts of the requested observed UUIDs, increments the local
component, and publishes chosen bytes. A later unobserved concurrent sibling
survives. As with ordinary vector clocks, newer events on the same node share
that node's causal counter order; there is no operation-level dotted clock.

## Atomicity and migration

Manifest encoding 2 marks causal versions. The `vectors` family maps version
UUIDs to contexts; a new `heads` family maps full object/version keys to visible
vectors. One synchronous WriteBatch publishes the manifest, vector, head-index
changes, committed-version index and staging removal after chunk WAL sync.
Readers never see a manifest without its vector or a partially updated sibling
set. Superseded data stays immutable and readable by UUID; no garbage collection.

Startup/checkpoint/restore audit chunks, counters, vectors and head-index
consistency. Object history scans stream one metadata record at a time; at most
33 head candidates are evaluated. All operations are serialized per store.
History checking grows with retained history and startup validates every object;
these are correctness-first operations, not a throughput claim.

Q1 manifests remain readable as legacy local versions. Causal and legacy writes
cannot share the same object key, including overlapping uploads. Copy legacy
content to a new causal key explicitly; never invent historical causality.
Opening a Q1 volume adds the heads family; Q1 binaries cannot safely reopen it.
Rollback requires a pre-upgrade backup and the matching original configuration.
A restored old checkpoint cannot safely originate new causal writes if any later
counter values escaped that checkpoint. Until reconciliation exists, restore
only a latest checkpoint with no later writes, and never run two copies with the
same node identity. Ordinary restart preserves the current durable counter.

## Alternatives and boundaries

Rejected last-write-wins because it discards concurrency; rejected assigning
vectors to legacy versions because their causal relationships are unknown;
rejected an independent metadata database because it would split publication
across two transactions. No public/internal object RPC or request-id idempotency
promise is added. Local replicas in tests use the storage ingestion API; these
are not network replication or partition-convergence claims.
