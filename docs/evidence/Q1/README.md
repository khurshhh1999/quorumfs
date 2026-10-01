# Phase 2 / Q1 verification — 2026-09-30

Base: merged PR #1, main commit `f32e8aa`. Working branch:
`codex/pr-2`. Changes are uncommitted; no remote CI result is claimed.

Environment: macOS ARM64, Temurin 21.0.12.1+1, Gradle 9.8.0, RocksDB JNI 10.10.1,
Docker 29.4.0 with Linux ARM64 containers. Dependency pins remain unchanged.
Storage test seed: 20260930. Existing cluster fixtures retain seed 20260928.

## Acceptance evidence

| Requirement | Verification |
| --- | --- |
| Durable local acknowledgment | Multi-chunk and zero-byte versions survive reopen; real process kill after publication/ack preserves bytes |
| Invisible partial uploads | Real kills during staging, before WAL sync and before manifest publication leave no visible version |
| No corrupt data returned as valid | Chunk corruption, missing chunks, recomputed chunk hash with incorrect whole digest, and corrupt checkpoints rejected |
| Safe recovery | Manifest/index/counter validation; staged cleanup cannot delete a committed version |
| Durable counters | Cancelled and interrupted uploads consume sequences; restart never reuses them |
| Bounded streaming | 64 MiB object uploaded/read in a separate JVM with maxHeap=25165824 (24 MiB); default eight-upload cap |
| Concurrent immutable versions | Eight simultaneous uploads all retained and individually verified |
| Cancellation/limits | Bad offsets, duplicates, truncated streams, size/hash errors, interruption, TTL and capacity covered |
| Disk-full/sync failures | Native Linux 8 MiB tmpfs produces IOError(NoSpace), acknowledged=false; deterministic injection at six persistence boundaries tests sync/I/O error propagation |
| Backup/restore | Checkpoint restores content/identity/counter; wrong identity, existing destination and corrupted backup fail without replacing target |
| Q0 migration | Identity-only database upgrades and preserves identity; mismatched identity rejected |

## Commands and results

- `./gradlew check assemble faultTest`: passed. 24 unit tests plus two separate-JVM
  tests (five kill scenarios and a bounded-heap scenario), CLI and five-node tests.
- Source-only copy: `./gradlew check assemble faultTest --no-build-cache`: passed.
  57 tasks executed successfully. This validates uncommitted source without reusing generated outputs; it is not
  a claim about remote CI or a release checkout.
- `python3 tests/system/storage_disk_full.py`: passed with native
  `IOError(NoSpace)` / `No space left on device`, no acknowledgment.
- `./gradlew storageIntegrationTest`: passed for 0, 1, 262144 and 262145 byte
  objects, restart/reopen, restore and identity/overwrite rejection.
- README local put/get/verify example: passed; output byte-for-byte identical.
- Compose image build/start: passed with five healthy nodes using existing volumes.
- `./gradlew smokeTest` and `python3 tests/system/proxy_fault.py`: passed.
- Spotless, warnings-as-errors compilation and descriptor compatibility: passed.
- actionlint 1.7.12: passed; shellcheck integration not installed/run.
- `git diff --check` and artifact exclusion audit: passed.

Generated XML/JSON, process logs and security reports are local-only in
`modules/*/build/test-results/`, `build/reports/system/`, and
`build/reports/security/phase2/`. These are ignored and must not be staged.

## Security follow-up

The initial image scan found CVE-2026-84782 in inherited libssl3/openssl
3.0.2-0ubuntu1.29. Dockerfile pins both packages to Ubuntu's fixed
3.0.2-0ubuntu1.30; no exception or suppression was added.
[Ubuntu advisory](https://ubuntu.com/security/CVE-2026-84782).
Final patched image scan: passed, zero HIGH/CRITICAL findings. Secret scan: passed, zero findings. CycloneDX SBOM: generated locally (191 components), ignored by Git. Patched Compose smoke and proxy tests also passed.

## Limits and unrun checks

Linux x86_64 remote CI remains pending. Physical fsync failure, disk destruction
and hardware power loss were not tested; sync-error propagation uses deterministic
hooks and ENOSPC uses a real filesystem. Java-heap evidence is not a total-RSS or
performance benchmark. Q2–Q8 distributed behavior remains unimplemented. Local
storage commands are offline; public object RPC readiness remains false.

## Changed files

- `.github/workflows/ci.yml`
- `Dockerfile`
- `README.md`
- `build.gradle`
- `delivery.md`
- `docs/adr/0003-durable-local-storage.md`
- `docs/evidence/Q1/README.md`
- `docs/handoffs/phase-2-commands.md`
- `docs/handoffs/phase-2-pr.md`
- `docs/runbooks/storage.md`
- `implementation.md`
- `modules/server/src/main/java/io/quorumfs/server/ServerMain.java`
- `modules/server/src/main/java/io/quorumfs/server/StorageMain.java`
- `modules/storage/build.gradle`
- `modules/storage/src/main/java/io/quorumfs/storage/ObjectStorage.java`
- `modules/storage/src/main/java/io/quorumfs/storage/StorageException.java`
- `modules/storage/src/main/java/io/quorumfs/storage/StorageKeys.java`
- `modules/storage/src/test/java/io/quorumfs/storage/ObjectStorageTest.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcess.java`
- `modules/storage/src/test/java/io/quorumfs/storage/StorageProcessTest.java`
- `plan.md`
- `tests/system/storage_cli.py`
- `tests/system/storage_disk_full.py`
