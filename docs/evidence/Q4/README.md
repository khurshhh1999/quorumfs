# Phase 5 / Q4 local evidence

Date: 2026-10-08 (America/New_York). Branch `pr-5`, based on `origin/main`
`a43fc7c`. Owner publication and remote CI for the eventual commit remain pending.

Environment: macOS ARM64, Temurin 21.0.12.1+1, Gradle 9.8.0, RocksDB JNI
10.10.1, Docker Engine 29.4.0 and pinned Trivy 0.74.0. No dependency changes.

## Tests and acceptance

53 unit/property tests, 18 five-node network integration tests and six storage
process tests pass with zero failures, errors or skips (77 total). Existing
ring/vector/protobuf/authentication and strict-quorum checks remain enabled.

New evidence includes:

- Tombstone dominance, late stale replay without resurrection, concurrent live
  siblings, explicit resolution, historical retention and checkpoint restore.
- Empty live/tombstone identity mismatch rejection, malformed tombstone rejection,
  idempotent ApplyTombstone and DeliverHint replay, and recreation using context.
- An offline canonical owner receives a deletion after coordinator restart,
  without needing a foreground read to drive catch-up.
- Read repair captures live/tombstone siblings from a real quorum, persists them
  while a target is disconnected, and completes after coordinator restart/heal.
- A full 128-version hint queue rejects a public write before owner publication.
- Partial acknowledgments, payload pinning, duplicate destination merge,
  checkpoint/restart preservation, malformed/truncated capture cleanup and
  fail-closed startup on corrupt pinned payloads.
- Prior storage-format marker migration preserves live causal history and writes
  the new marker. In-place downgrade is intentionally unsupported.
- Nineteen separate JVM kills: five legacy, five causal-live and five tombstone
  publication boundaries, plus four hint publication/acknowledgment boundaries.
  Each restart audits visible versions or pending work against the commit boundary.
- A 24 MiB-heap child streams four 64 MiB hint payloads (256 MiB pinned total),
  verifies them and rejects a fifth at the byte budget. The earlier maximum-object
  bounded-heap test also remains passing. Process seeds remain 20260930; new
  recovery payload seed is 20261008.

The five-JVM public CLI test adds delete, tombstone metadata, failed deleted
GET without an output file, killed-owner restart and recreation with context.
The five-container authenticated CLI fixture verifies deletion/recreation too.
Both retain prior upload/download/resolve checks. Existing bootstrap restart,
identity mismatch, process kill and offline causal/storage CLI checks remain.

## Commands and execution records

Use JDK 21 in `JAVA_HOME`:

```bash
./gradlew spotlessApply check faultTest assemble :client:installDist
./gradlew spotlessApply :server:installDist :client:installDist
python3 tests/system/storage_disk_full.py
python3 tests/system/quorum_compose.py
```

The full working-tree Gradle run passed. Native RocksDB on an 8 MiB Linux tmpfs
reported IOError(NoSpace), with `acknowledged=false`. The authenticated Docker
fixture passed and stopped containers while preserving volumes.

Fresh source-only copy (no generated inputs, no Gradle build cache):
`./gradlew --no-build-cache check faultTest assemble :client:installDist` passed
in 3m 18s, with all 68 tasks executed. All 77 Java tests passed there, as did
five-JVM CLI, offline storage/causal CLI, bootstrap restart and process-kill tests.
All 74 module/test/infrastructure/build source inputs were byte-compared with the
working tree afterward and matched.

Additional verification passed:

- Actual previous Phase 4 distribution created a format-1 causal object; the new
  distribution upgraded and downloaded identical bytes. The old distribution
  then refused the format-2 database. Runtime artifact: `phase5-upgrade.json`.
- Docker bootstrap `./gradlew smokeTest` and `python3 tests/system/proxy_fault.py`:
  five healthy nodes, disconnect/reconnect and independent healthy-node checks.
- Trivy image scan: zero HIGH/CRITICAL findings; source secret scan: zero findings.
  A CycloneDX SBOM was generated. No suppression or dependency change was needed.
- Actionlint, Compose configuration validation, Python syntax and `git diff --check`.
- Explicit staging dry-run: 30 intended source/test/Markdown/ignore files; no
  mutation. No tracked files match artifact ignore rules. All four root local-only
  files remain present, ignored and untracked; the Git index is empty.
- Both container fixtures were stopped after validation without deleting volumes.

Fresh-build logs are `.tools/pr-5-clean-check.log`; working-tree logs are
`build/phase5-*.log`. Security JSON/SBOM and upgrade records are in `build/reports/`,
with system fixture results beneath `build/reports/system/`. These paths remain
ignored. The source-only copy is not a Git checkout or a remote CI result.

## Artifacts and limitations

Raw Gradle XML, JSON, logs, container evidence and SBOM stay ignored under build
folders and `.tools/`. Only source and Markdown summaries belong in this change.
No local planning or agent-instruction file is tracked, staged or included in
staging commands. The index remains untouched by this implementation.

These tests cover declared process/partition faults with preserved volumes, not
hardware power loss, disk destruction or the later 10,000-operation campaign.
Read-repair discovery before durable capture is best effort; already captured
work is durable. Hints never count toward W and never expire; full queues block
new writes. Round-robin delivery and capped backoff bound work without discarding
pending records. Tombstones and old object history are retained indefinitely.
Anti-entropy, mTLS and production operational hardening remain later phases.
Storage format 2 requires a coordinated upgrade; see ADR 0006 and the recovery
runbook for backup/rollback constraints.
