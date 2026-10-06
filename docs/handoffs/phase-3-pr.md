## PR 3: consistent hashing and causal versions

Adds deterministic ownership for the five-node configuration and durable causal
versions to local storage. Concurrent writes remain visible as siblings; explicit
resolution merges only observed contexts and preserves racing concurrent writes.

- SHA-256/128 ring, 128 vnodes per node, distinct physical owners, canonical
  persisted vnode map, strict epoch/config compatibility and startup fingerprint.
- Immutable vector algebra, durable counter increments, 32-sibling limit,
  equal-vector integrity checks and atomic manifest/vector/head publication.
- Offline owners, causal writes, sibling inspection and resolution commands.
- Property tests, native storage/corruption/restore tests, five-process ownership
  comparison and real process kills around causal publication.

Validation and exact local commands: `docs/evidence/Q2/README.md`.
Remote CI at this new head remains pending until the owner pushes.

Upgrade adds a heads column family and causal manifest encoding. Legacy versions
remain readable; causal writes require separate keys. Back up before upgrading;
rollback requires a pre-upgrade backup. Never originate writes from a stale
checkpoint or run two restored copies with the same node identity.

Distributed object RPCs still return UNIMPLEMENTED. No quorum acknowledgment,
network replication, repair, authentication, or performance claim is introduced.
Generated artifacts remain ignored; the change contains source and Markdown only.
