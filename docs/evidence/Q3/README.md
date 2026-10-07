# Phase 4 / Q3 local evidence

Date: 2026-10-07. Branch `pr-4`, based on `origin/main` at `51c6f3d`
(merged PR #3). Working-tree verification only; owner commit/push/PR and remote
CI for the new head remain pending.

Environment: macOS ARM64, Temurin 21.0.12.1+1, Gradle 9.8.0,
RocksDB JNI 10.10.1, Docker Engine 29.4.0, Trivy 0.74.0.
Runtime dependency versions remain pinned; coordinator/replication dependency
locks now include their existing shared project dependencies.

## Reproducible checks

- `./gradlew spotlessApply check faultTest assemble :client:installDist` passed.
- `./gradlew spotlessApply :server:quorumIntegrationTest :server:installDist
  :client:installDist assemble` passed after adding replay/read-identity tests.
- 45 unit/property tests and three process tests pass with zero failures,
  errors or skipped cases. Coverage retains all earlier storage/vector/ring/
  protobuf compatibility checks and adds bearer policy, wire identity/metadata,
  coordinator-counter allocation and preservation of replicated UUIDs.
- 13 quorum integration tests use five real Netty gRPC servers, native RocksDB
  databases and 25 independently cuttable directed TCP links. Seed 20261007.
  They validate:
  - W canonical durable copies, non-owner coordination, owner failure and restart;
  - a real 2/1 owner network partition, majority progress, minority read/write
    rejection and bounded reconnect after healing;
  - concurrent writes, conflict metadata, selected reads and explicit resolution;
  - slow acknowledgments, rejection of wrong node/UUID/epoch/non-durable replies;
  - typed unknown outcomes despite durable partial publication;
  - namespace/client/peer credential separation and epoch/same-epoch rejection;
  - corrupt/truncated uploads, cancellation, admission exhaustion and slot cleanup;
  - large streamed objects, valid quorum misses, corrupt fetch rejection;
  - replica replay identity and invalid read identities not counting toward R.
- `quorumCliTest` passes against five independent JVMs and volumes: real CLI
  put/get/head/context/resolve/owners, authorization denial, exclusive download
  publication and reads after an owner process kill. This runs under `check`.
- Existing five-JVM bootstrap restart/kill checks, causal/offline CLI tests and
  all ten storage publication kill boundaries remain passing. Storage process
  seed 20260930; bootstrap/ring legacy seed 20260928.
- `python3 tests/system/quorum_compose.py` passes with all five authenticated
  containers and the proxy. Credentials are generated at runtime, never logged
  or checked in. The fixture stops containers afterward and preserves volumes.
- `python3 tests/system/storage_disk_full.py` passes: native
  `IOError(NoSpace)`, 8 MiB tmpfs, 24 MiB heap, no false acknowledgment.
- Fresh source-only copy: `./gradlew --no-build-cache check faultTest assemble
  :client:installDist` passed, all 68 tasks executed, all 61 Java tests passing
  with zero failures/errors/skips, plus process/CLI integration. No old build
  outputs or Gradle build cache were used.
- README setup/upload/download/context/resolve/health/shutdown commands passed
  using that fresh distribution against Docker. Gradle commands had already
  passed in the same fresh source tree and were not redundantly rerun.
- Trivy 0.74.0 image scan: zero HIGH/CRITICAL findings; source secret scan: zero
  findings. CycloneDX SBOM generated in ignored reports; no suppressions added.
- Actionlint, Compose configuration validation and `git diff --check` pass.
- Explicit-path staging dry-run contains 40 intended source/test/docs/build-input
  files. No tracked file matches artifact ignore rules; the index remains empty.
  Docker fixtures were stopped after validation without deleting volumes.

## Artifacts and limits

Raw XML/JSON/logs/SBOM stay ignored in `build/reports/`, module build directories
and `.tools/pr-4-*.log`. Source tests and Markdown summaries are the reproducible
handoff. No generated binary, container artifact, credential or report is staged.

This is not the later 10,000-operation campaign or a throughput/power-loss proof.
The tests demonstrate the declared process/partition model with preserved volumes.
Data RPCs require development bearer settings; transport is explicitly plaintext.
Individual principals, mTLS, deletion, hints, repair and anti-entropy remain later
milestones. Successful writes may exist on W rather than all N owners; a returning
stale owner does not automatically catch up yet. Stale-checkpoint counter reuse
remains unsupported. Detailed migration and failure semantics are in ADR 0005
and `docs/runbooks/quorum.md`.
