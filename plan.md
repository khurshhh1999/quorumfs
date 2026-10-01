# QuorumFS execution plan

## Objective and boundaries
Build a five-node leaderless object store using Java, gRPC, RocksDB and Docker, with consistent hashing, chunked streaming, version vectors, configurable strict quorums, hinted handoff, read repair, Merkle anti-entropy, checksums, tombstones and fail-closed behavior. Validate with a reproducible campaign of at least 10,000 operations.

Phase 1 / Q0 is merged in PR #1. Phase 2 / Q1 implements durable local storage and passes local validation; owner commit/push and remote CI are pending. Q2–Q8 remain pending. `implementation.md` is the detailed design; `delivery.md` supplies CI/CD and owner-run Git/PR rules. No POSIX filesystem, S3 compatibility, dynamic membership, linearizability, or multi-region promise is in v1 scope.

## Ordered milestones and PR slices

### Q0 - Build and contracts
- [x] Dedicated repository; pinned Java/Gradle/gRPC/protobuf/RocksDB versions, formatter, static checks and command contract.
- [x] Public/internal protobufs, typed error mapping, versioning policy and module skeleton.
- [x] Five-node Docker Compose with persistent separate volumes, stable node identities, health checks, fault proxy and deterministic fixtures.
- [x] CI, PR template and ADRs for consistency, strict quorum and fault model.
Evidence: `docs/evidence/Q0/README.md`. Pinned build, formatter, warnings-as-errors, command tasks and dependency locks are implemented. The owner has now initialized the dedicated repository. CI workflow is implemented and linted, but remote execution/protection is owner-pending.

Acceptance: clean checkout build and five process health checks; protobuf compatibility gate; all containers use unique stable identities. Suggested PR: `build: bootstrap five-node storage workspace`.

### Q1 - Durable single-node storage (depends Q0)
- [x] RocksDB column families, versioned key encoding, native resource management and durable counters.
- [x] Streaming staging, chunk/whole-object checksum verification, atomic manifest publication, cancellation cleanup and size limits.
- [x] Restart recovery, disk-full/sync-error paths and backup/restore test.
Evidence: `docs/evidence/Q1/README.md`. Local immutable versions and offline CLI only; distributed RPC readiness remains false.

Acceptance: acknowledged object survives restart; crashes before commit expose no partial object; missing/corrupt chunks are never served as valid; memory usage bounded by stream concurrency rather than full object size.

### Q2 - Ring and causal versions (depends Q1)
- [ ] Consistent hash ring with distinct physical owners, virtual nodes, stable epoch/config validation and deterministic serialization.
- [ ] Vector dominance/equality/concurrency, durable increments, sibling handling and explicit resolution.
- [ ] Property tests for ring determinism, replica uniqueness, vector algebra and convergence independent of merge order.
Acceptance: five nodes compute identical owners; invalid/unequal epochs fail closed; incomparable writes remain visible; counter identity survives restart. Suggested PRs: hashing; causal versions.

### Q3 - Quorum coordinator (depends Q2)
- [ ] Replication/read RPCs with deadlines, canonical-owner response counting and complete durable write acknowledgments.
- [ ] R/W/N validation, partial-success uncertainty, conflict metadata, context-aware client/CLI and authorization.
- [ ] Majority/minority partition tests, delayed replica tests and concurrent overlapping writes.
Acceptance: success requires W durable canonical owners; read requires R valid replies; hints never count; minority side cannot silently weaken guarantees; timeout can honestly report unknown outcome.

### Q4 - Deletion and recovery (depends Q3)
- [ ] Causal tombstones, explicit delete conflicts and safe version retention; no time-only tombstone purge.
- [ ] Durable hints with pinned payloads, retry/ack lifecycle, capacity bounds and observability.
- [ ] Bounded read repair preserving siblings and tombstones.
Acceptance: offline node catches up without resurrecting deleted versions; hint replay is idempotent; concurrent delete/write remains conflict; crash during repair preserves committed data. Suggested PRs: tombstones; hints/read repair.

### Q5 - Merkle anti-entropy (depends Q4)
- [ ] Snapshot-based deterministic trees/ranges with metadata covering every sibling and tombstone.
- [ ] Divergence discovery, bounded range exchange, verified version transfer and restartable rounds.
- [ ] Mutation-during-scan, expired/missing hint and corrupted-replica scenarios.
Acceptance: healed replicas converge within fixture deadline without foreground reads, even when hints are unavailable; no concurrent version is silently dropped; mismatched epochs cannot repair against each other.

### Q6 - Operational hardening (depends Q5)
- [ ] mTLS, client namespace permissions, key rotation, stream admission and repair budgets.
- [ ] Metrics, dashboards, failure alerts, readiness/liveness and graceful shutdown.
- [ ] Backup/restore, failed-node recovery, corruption, disk pressure, partition and rolling-upgrade runbooks.
Acceptance: unauthorized namespace access fails; resource exhaustion is bounded; restore verifies data plus vectors/tombstones/identity; a mixed compatible-version cluster passes smoke.

### Q7 - Fault campaign and validation (depends Q6)
- [ ] Durable independent operation ledger and causal-history oracle; deterministic scheduling and fault hooks.
- [ ] Execute >=10,000 operations with required workload and fault matrix; preserve seed/history/config and verify each acknowledged version or its valid causal successor.
- [ ] Repeat campaign with three seeds and run larger objects/boundary sizes separately; measure streaming memory and recovery overhead.
Acceptance: zero observed acknowledged-version loss under the explicit supported model, correct unavailable-quorum responses, no unchecked corruption served, and convergence before deadline. Any failure remains open with a minimal reproducible history. Record result counts, not just a green test label.

### Q8 - Release and owner handoff (depends Q0-Q7)
- [ ] Full CI and nightly-equivalent fault suite; clean installation, restart and upgrade smoke.
- [ ] Customer README with tested CLI commands, API examples, conflict resolution, limits and accurate failure semantics.
- [ ] Owner visibility decision for internal docs; release notes, signed immutable images, evidence manifest and concrete PR handoff.
Acceptance: runnable five-node demo from a clean checkout; implementation matches stated semantics; owner receives exact commit/push/PR/check/merge commands. No agent-run commit, push, merge or release.

## Required fault matrix

| Scenario | Expected behavior |
| --- | --- |
| Kill coordinator before W acknowledgments | No success response; partial state allowed, recoverable and explicit |
| Kill after durable W but before client response | Client sees unknown outcome; durable version survives |
| Kill replica after successful PUT | Remaining copies preserve acknowledged version within stated model |
| Canonical owners split 2/1 with R=W=2 | Side reaching two may proceed; side reaching one fails quorum |
| Two of three owners unavailable | Writes/reads fail closed when their configured quorum is impossible |
| Concurrent updates without shared context | Multiple siblings exposed; no silent overwrite |
| Delete while replica offline | Tombstone propagates; returning stale node cannot resurrect data |
| Drop hints and heal partition | Anti-entropy repairs remaining divergence |
| Bit corruption/truncated stream | Reject/quarantine; repair only from verified copy |
| Disk full/fsync failure | No false durable acknowledgment |
| Different membership epoch | Reject incompatible operation/repair |
| Restart during upload/repair | No partially visible object and no committed data garbage collection |
| Max-size objects and stream flood | Bounded memory, backpressure and explicit resource exhaustion |

## Evidence and completion tracking
Create `docs/evidence/Qx/` containing command/results, environment, seed, history manifest and remaining limitations. Keep large histories outside Git with checksums and accessible artifact references. Current tracking:

| Milestone | Status | PR | Evidence |
| --- | --- | --- | --- |
| Q0 / Phase 1 | Merged | #1 | `docs/evidence/Q0/README.md` |
| Q1 / Phase 2 | Locally verified; owner handoff and remote CI pending | Not created | `docs/evidence/Q1/README.md` |
| Q2–Q8 | Not started | None | None |

The clean-build evidence is a source-only copy, not a Git checkout, because dedicated repository initialization is owner-controlled. Do not backfill results from source text or infer that local process crashes validate hardware power-loss durability.

## Definition of done
All source features work together on five nodes; contracts specify causal conflicts and uncertain outcomes; strict quorums and durable acknowledgments are verified; healing converges without data resurrection; the 10,000-operation campaign has inspectable evidence; CI/CD and recovery instructions work; README is truthful and customer-facing; and the user controls the final Git and release actions.
