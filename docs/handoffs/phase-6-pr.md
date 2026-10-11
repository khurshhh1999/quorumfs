## PR 6: Merkle anti-entropy

Adds periodic replica reconciliation without hints or foreground reads. Fixed
Merkle ranges compare immutable RocksDB snapshots of shared canonical-owner
heads, including concurrent siblings, tombstones and verified payload health.
Divergent leaves use bounded paginated metadata exchange and verified, idempotent
version transfer. Restart begins a fresh comparison while retaining all completed
replica publications. Peer/leaf/entry rotation prevents exhausted rounds from
permanently starving later work.

Matching version metadata can now expose damaged payloads through the health bit.
A verified replacement is written under new chunks and atomically published by
switching the manifest pointer. UUID, vector, digest and causal heads stay intact;
invalid or interrupted repair cannot overwrite committed bytes.

Validation: 57 unit/property tests, 23 real-network integration tests and eight
process tests (88 total), including hint-free partition/restart convergence,
concurrent deletion/write preservation, corruption repair, snapshot mutation and
expiry, paginated budget continuation, five repair publication kill boundaries
and 64 MiB repair with a 24 MiB heap. Full commands, clean-build/container/security
results and limitations are in `docs/evidence/Q5/README.md`. Remote CI for the
owner's eventual commit remains pending.

Four additive protobuf fields bind peer snapshots and report payload health;
existing field numbers and RPC signatures are unchanged. Storage marker 3 reads
and upgrades markers 1/2 and adds replacement-payload manifests/reservations
without a new column family or dependency. Back up and stop all five nodes for
coordinated upgrade. Prior binaries reject format 3; in-place downgrade and
mixed-version operation are unsupported. See ADR 0007 and the anti-entropy
runbook for budgets, recovery and rollback.

Large snapshots fail explicitly at the documented preview bounds. Native DB or
metadata corruption and startup audit failures still require operator recovery.
No tombstone/history garbage collection, production transport hardening, release
or performance guarantee is introduced. Planning documents and execution artifacts
remain local and ignored. The owner controls commit, push, PR creation and merge.
