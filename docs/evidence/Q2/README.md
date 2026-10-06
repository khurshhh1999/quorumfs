# Q2 / Phase 3 local evidence

Date: 2026-10-06. Branch: `pr-3`; base `origin/main` at `5560c6c` (PR #2 merged).
These are local working-tree results, not remote CI or a published PR.
Environment: macOS ARM64, Temurin 21.0.12.1+1, Gradle 9.8.0,
RocksDB JNI 10.10.1, Docker Engine 29.4.0. No runtime dependencies added.

## Checks

- `./gradlew spotlessApply check faultTest assemble :client:installDist --write-locks`:
  passed; lockfiles unchanged. Later additional corruption/order tests also pass
  with `./gradlew spotlessApply test`.
- 41 unit/property tests, zero failures/errors/skips. Four ring tests include
  50,000 owner comparisons across five independently constructed configurations,
  all N=1..5, deterministic member permutations, and a Python-derived golden hash.
  Four vector tests include 10,000 algebra trials and 500 merge-order/batching
  histories. Q2 randomized seed: 20261006.
- Nine causal native-storage tests cover concurrent siblings and racing
  resolution, restart/counter floors/overflow, backup/restore, same-vector digest
  mismatch including superseded history, 32-sibling capacity, epoch/configuration
  rejection, legacy isolation, corruption rejection and arrival-order convergence.
- Three process tests include ten separate-JVM kills (legacy and causal variants
  of staged/WAL-sync/pre-manifest/post-manifest/acknowledged boundaries) and a
  64 MiB object streamed with a 24 MiB Java heap. Existing process seed: 20260930.
  Only the two post-publication boundaries expose committed content and causal
  metadata; next node counter is 2 after every causal boundary.
- `check` runs legacy CLI and causal CLI integration: verified boundary-sized
  objects, backup/restore, five independent offline processes with identical
  ownership, explicit sibling resolution and epoch/same-epoch drift rejection.
  The live five-node gRPC suite verifies health, distinct identities, unchanged
  UNIMPLEMENTED data RPCs, restarts and invalid-epoch startup rejection.
- `faultTest` additionally kills/restarts a live gRPC node while peers stay healthy.
- `python3 tests/system/storage_disk_full.py`: passed with native
  `IOError(NoSpace)`, no acknowledgment, 8 MiB tmpfs and 24 MiB heap.
- Docker Compose build/start: all five nodes and proxy healthy. `./gradlew
  smokeTest` passed; `python3 tests/system/proxy_fault.py` passed with JDK 21.
  The first standalone proxy invocation inherited the older default Java and
  failed its initial health assertion; rerunning with the configured JAVA_HOME
  passed. No fault was injected by that failed invocation.
- Fresh source-only copy: `./gradlew --no-build-cache check faultTest assemble
  :client:installDist` passed, 64 tasks executed, including all 41 unit/property
  and three process tests. No existing build outputs or build cache were used.
  The README local-storage and causal runbook command examples passed there.
- Trivy 0.74.0 refreshed its vulnerability databases: the built image has zero
  HIGH/CRITICAL findings; the source-only secret scan has zero findings.
  CycloneDX SBOM generated into ignored reports. No suppressions added.
- Actionlint, Compose configuration validation and `git diff --check`: passed.
- Explicit-path `git add --dry-run` includes only the 28 intended source/test/
  Markdown files. No tracked file matches artifact ignore rules; index is empty.
  Compose was stopped after verification without deleting its data volumes.

## Evidence location and limits

Raw results stay ignored under `build/reports/`, module `build/test-results/`, and
`.tools/pr-3-*.log`. Source tests reproduce the checks; no report JSON/XML, image,
archive, local tools or build binary is part of the staging instructions.

No hardware power-loss durability proof, network replication, quorum operations,
anti-entropy, reconfiguration, benchmark or 10,000-operation distributed campaign
is claimed. Store operations serialize; historical metadata scans grow with
retained history. Backup must contain the latest counter before originating new
writes; stale-counter reconciliation is deferred. Local replica convergence tests
compare vector/content sets, while local UUIDs identify immutable local records.
