# ADR 0006: Causal deletion and durable recovery

Status: accepted for the development preview.

## Decision

A delete is an immutable zero-byte version with the SHA-256 of empty bytes and
`tombstone=true`. It uses the supplied causal context, a durable coordinator
counter, and W validated canonical-owner acknowledgments. Empty live objects and
tombstones are distinct identities even when their vector and digest match;
such inconsistent identities are rejected. All immutable history and tombstones
are retained indefinitely. There is no time-based purge.

HEAD returns all maximal versions, including tombstones, so clients can obtain
context to recreate an object. GET returns NOT_FOUND for a selected tombstone or
a single tombstone head. Multiple maximal heads return conflict metadata,
including concurrent live/deleted versions. Resolution can choose live bytes or
issue another deletion with merged context. Historical offline reads remain
available; deletion is causal visibility, not secure erasure.

Before dispatching a write, the coordinator captures one private payload copy
and delivery intents for every canonical owner in RocksDB's reserved `hints`
family. Chunk writes precede synchronous WAL publication of the intent. No hint
is a visible object head or a quorum acknowledgment. A validated durable owner
ack removes only that destination; the last ack atomically removes the intent
and its payload. A lost reply causes idempotent replay with the original UUID,
vector, digest and tombstone bit. Unknown writes can subsequently complete via
handoff; callers must inspect context before retrying.

A node holds at most 128 pending versions and 256 MiB of pinned hint payloads.
Capacity is checked before dispatch, including healthy writes, to keep the
recovery promise independent of a race with an owner failure. Exceeding either
budget returns RESOURCE_EXHAUSTED before any replica publication. This deliberately
trades write availability under a full backlog for bounded, durable recovery.
Pending hints do not expire. A permanent coordinator volume loss can still lose
its hints; anti-entropy is the next milestone.

One background worker handles at most one payload per second plus RPC time,
with at most N destination attempts and a 64 MiB object limit per cycle. RPCs have
three-second deadlines. Per-destination exponential delays run from 2 to 64
seconds; retries continue indefinitely. Restart resets the delay, not the durable
work. Shutdown cancels and joins recovery workers before closing storage.

Quorum reads trigger best-effort repair discovery with two concurrent preparation
slots and no waiting queue. Each missing maximal version is fetched and verified
from an observed holder, then captured in the same durable hint queue. Already
captured work survives restart. A crash before capture, saturated preparation,
or a full hint budget defers discovery to a later read; it never claims repair
completion. Repair applies the same causal merge as foreground replication,
retaining concurrent siblings and refusing inconsistent identities. It is not
anti-entropy: unread divergence without surviving hints can remain.

## Format and compatibility

Opening with this binary upgrades the RocksDB format marker from 1 to 2 using a
synchronous write. Existing legacy/live manifest encodings 1 and 2 are unchanged;
manifest encoding 3 denotes a causal tombstone with the same fixed fields.
Hint metadata has its own version byte, destination list, opaque replica header,
size, digest and creation timestamp; its payload chunks use a separate key space
within the hints family. Startup removes only unpublished hint chunks and audits
all published payload checksums, bounds and destination identities. Checkpoints
and restores include the hints family, tombstones, vectors and node counters.
No new column families, dependencies, or protobuf field numbers are introduced.

Old binaries reject database format 2 instead of silently ignoring recovery work.
This requires a coordinated maintenance upgrade of all five nodes with verified
backups; mixed-version data service and in-place downgrade are unsupported.
A pre-upgrade backup is not permission to reuse an obsolete causal counter against
a newer cluster. Rollback requires a separately verified whole-cluster recovery
point and reconciliation of later acknowledged writes.

## Alternatives and limits

Time-only tombstone collection risks resurrection. Memory-only hints lose work on
restart. Non-owner temporary replicas cannot satisfy W. Copying entire payloads
into one write batch would violate the bounded-heap requirement. These alternatives
are rejected. The selected queue streams chunks with one chunk-sized buffer;
temporary transfer space adds up to three recovery objects (192 MiB) per node on
top of foreground staging. Retained history and native WAL/compaction overhead
still require disk monitoring; the logical queue bound is not a filesystem quota.
