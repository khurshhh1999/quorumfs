# ADR 0005: Canonical-owner quorum RPCs

Status: Phase 4 / Q3. Five fixed nodes; no leader and no membership changes.

## Acknowledgment boundary

Public PutObject/ResolveObject validate a header, namespace permission, positive
bounded vector components, size and request ID, then stream checksummed fixed-size
chunks into a private temporary file. The complete size and digest must match
before replication starts. This temporary file is not a committed version or a
replica acknowledgment, including when the coordinator is a non-owner.

A W-owner metadata preflight rejects already-unavailable quorums before dispatch.
The coordinator durably allocates its causal component without publishing a local
object, assigns one UUID and rejects known sibling-capacity exhaustion. It sends
the same version to the N canonical owners concurrently. Each receiver validates
the full cluster configuration, ownership, immutable identity and content; it
publishes the UUID, vector, chunks and head set using the existing synchronous
storage transaction. Only then may its reply say durable=true.

Count at most one result per contacted canonical identity. Validate reply node,
cluster/configuration, UUID and durable flag. A non-owner's temporary copy, hint,
wrong identity, malformed reply, failed RPC or incomplete stream never counts.
Return success immediately after W valid completions; cancel remaining work.
Cancelled replicas can already have committed. Consequently successful writes
may reach W rather than N owners. Hinted handoff and repair are later work.

Replica replay with the same UUID/object/vector/size/digest validates existing
bytes and returns the existing identity without reallocating or overwriting.
Concurrent duplicate ingestion may fail and require retry; request IDs do not
provide global exactly-once semantics. Phase 3 local replica ingestion retains
its API; the new network path explicitly preserves the coordinator UUID.

## Reads and conflicts

ReadVersions responds only for canonical owners with matching configuration and
verified complete local heads. Query owners concurrently and require R distinct
valid responses, including valid empty responses. Empty merged state becomes
NOT_FOUND only after this quorum. Merge all received metadata with vector algebra;
equal identities with inconsistent metadata are integrity failures. Exceeding
32 visible siblings fails with resource exhaustion rather than discarding data.

HeadObject returns the merged visible metadata. GetObject without a selected
UUID returns conflict metadata when multiple siblings remain. A selected UUID
must be in the observed merged head set. Fetch from a responding holder, verify
all chunks/offsets/size/digest into a bounded temporary file, then expose bytes.
Try other known holders on failure; never expose unchecked bytes. Superseded
historical UUIDs remain accessible through offline storage, not quorum GetObject.

Resolution carries explicitly observed merged context and chosen bytes. It does
not automatically absorb an unobserved racing write. `client context` obtains
and merges an R-quorum observation; `resolve` consumes that exact context.
No wall-clock winner, linearizability, serialized blind retries or snapshot
across different objects is promised.

## Deadlines, cancellation and resource bounds

Public client default: 10 seconds (configurable >0..15 seconds). The server caps
an upload at 15 seconds and honors shorter incoming deadlines. Peer RPCs have a
3-second cap within the operation deadline, with wait-for-ready reconnection.
Failed streams stop waiting for readiness as soon as their error arrives.
An upload with no further frames expires and is cleaned up. Caller cancellation
and terminal failure release stream slots and remove temporary files. Startup
removes only recognized abandoned temporary files after taking the DB lock.

There are eight admitted public operations and eight replica operations per node.
Manual inbound flow control and outbound transport readiness bound buffered
chunks (256 KiB) per stream. Object limit is 64 MiB. Temporary disk usage can be
up to 16 x 64 MiB across the two services, in addition to committed storage and
RocksDB upload staging. Large transfers on slow disks/networks may time out;
there is no maximum-size latency guarantee. Virtual worker tasks are bounded by
admitted operations times canonical owners; cancelled timers are removed.

Before replication, validation/authorization/capacity errors are explicit and
an unavailable preflight returns UNAVAILABLE. Once replica publication is
attempted, fewer than W validated acknowledgments returns DEADLINE_EXCEEDED with
OUTCOME_UNKNOWN details, even if some owners stored the version. A transport or
caller deadline can omit application trailers and must also be treated as an
unknown write outcome. Inspect quorum state before retrying with observed context.

## Authentication and enablement

The development data service is opt-in: all three environment settings must be
provided (`QUORUMFS_CLIENT_TOKEN`, `QUORUMFS_PEER_TOKEN`, `QUORUMFS_NAMESPACES`).
Different 32..256-character client/peer credentials are required. Partial/invalid
configuration fails startup. The client credential can access only the finite
namespace allowlist; the peer credential gates internal methods separately.
Comparison is constant-time and credentials are never logged or checked in.
Cluster discovery and bootstrap health remain public. With no settings, the
legacy bootstrap inspection mode remains available and object readiness is false.

This is bearer authentication over explicitly enabled plaintext development
transport. It is suitable only for a trusted local fixture. mTLS, individual
principals, read/write policy separation, rotation and comprehensive budgets
remain operational-hardening work. Delete, hints, read repair and Merkle RPCs
remain unimplemented. Object readiness indicates supported quorum methods are
enabled locally; it is not a quorum-availability or all-methods readiness claim.

## Compatibility and recovery

ClusterIdentity adds field 3 containing canonical ring bytes; existing fields
and RPC numbers remain unchanged and the baseline compatibility gate passes.
Internal operations require that field; all five nodes must be upgraded together
before enabling data service. No new RocksDB family/manifest format is added to
Phase 3. Never lower an epoch or erase a persisted identity to force a match.

Back up before upgrade. Stop writes and use latest per-node checkpoints with
matching identities; do not clone a running counter identity or resume from a
stale counter while later versions may exist. Local-only data is not automatically
redistributed. A returning stale replica does not catch up automatically yet.
Temporary transfer files are intentionally excluded from native checkpoints.

Rejected: counting coordinator staging toward W; accepting claimed peer identity
without matching the contacted owner; treating a missing single-node response as
NOT_FOUND; holding whole objects in heap; and hiding partial write uncertainty.
