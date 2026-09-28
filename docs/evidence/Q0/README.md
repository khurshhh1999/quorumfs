# Phase 1 / Q0 evidence — 2026-09-28

Scope: initial build/contracts/bootstrap milestone. No object storage, replication,
quorum coordination, vector clocks, or object durability has been implemented.

## Environment and pins

- Host: macOS 26.3.1, ARM64; Temurin JDK 21.0.12.1+1.
- Gradle 9.8.0; protobuf plugin 0.10.0; Spotless 8.10.3; google-java-format 1.33.0.
- gRPC 1.84.0; protobuf/protoc 4.36.2; RocksDB JNI 10.10.1; JUnit 6.1.3.
- Docker 29.4.0; Compose 5.1.2; Toxiproxy 2.12.0.
- Container: pinned Temurin Ubuntu Jammy JRE, confirmed Java 21.0.12.1+1.
- Trivy 0.74.0; actionlint 1.7.12. Downloaded local tools verified against release SHA-256 files.
- Deterministic fixture seed: 20260928. N=3, R=2, W=2, epoch=1, cluster=quorumfs-dev.
- No project Git SHA exists yet. `source-sha256.json` identifies the tested code/configuration.

## Passed checks

| Check | Result |
| --- | --- |
| Gradle wrapper JAR and distribution SHA-256 | Match upstream checksums |
| `./gradlew check assemble faultTest` | Passed with real native RocksDB and gRPC |
| Source-only copy: `./gradlew check assemble faultTest --no-build-cache` | 52 tasks executed; passed; no prior build outputs in copy |
| `./gradlew test` | 8 tests; 0 failures/errors/skips; quorum test covers all 343 R/W/N combinations in 0..6 |
| `./gradlew spotlessCheck` (also in `check`) | Passed |
| Java `-Xlint:all -Werror` | Passed |
| Descriptor compatibility against `docs/contracts/v1/` | Passed |
| `./gradlew integrationTest` (also in `check`) | Five unique nodes, restart identity preserved; changed epoch rejected |
| `./gradlew faultTest` | Process killed, unreachable endpoint detected, other nodes healthy, same identity recovered |
| `docker compose -f infra/compose/compose.yaml config --quiet` | Passed |
| `docker compose -f infra/compose/compose.yaml up --build --wait --wait-timeout 180` | Five node containers healthy |
| `./gradlew smokeTest` | All five proxy endpoints report expected identity/quorums; data RPC is UNIMPLEMENTED |
| `python3 tests/system/proxy_fault.py` | Disable node1 proxy, verify failure and node2 health, re-enable and verify recovery |
| Compose restart followed by `./gradlew smokeTest` | Passed; original identities retained |
| Docker inspect | Five distinct named volumes; healthy nodes; UID/GID 10001 |
| Trivy image HIGH/CRITICAL gate | Passed, zero findings in scanned application image |
| Trivy filesystem secret scan | Passed, zero findings (local .tools/.gradle excluded) |
| CycloneDX SBOM generation | Passed; report under `build/reports/security/` |
| actionlint workflow validation | Passed (shellcheck unavailable; shellcheck integration not run) |

Raw unit XML and compact system/volume/security summaries are local-only ignored files in this directory.
Full generated reports remain in ignored `build/reports/`, and CI uploads them for
14 days. Security report SHA-256 values identify the local reports; rerun scans to
obtain current findings. Security results are time-bounded, not a safety guarantee.

## Corrections found during validation

- Moved dependency resolution into each subproject for Gradle's project locking rules.
- Made the container's bounded temporary mount executable so RocksDB JNI can load;
  retained non-root execution and a read-only root filesystem.
- Used Adoptium's exact SemVer `21.0.12+101.0.LTS` in setup-java; the displayed JDK
  version is 21.0.12.1+1. Four-component Java version strings are not valid action inputs.
- Added dependency checksums for both macOS ARM64 and Linux x86_64 protoc/gRPC tools.

## Pending / unrun

Dedicated repository initialization, owner commits, remote CI on Linux x86_64,
branch protections, PR review and merge remain owner-controlled. A clean Git
checkout test is pending; the source-only clean copy passed. No remote CI result
is claimed. `benchmark` is intentionally unsupported, and no performance result
is claimed. Q1–Q8 acceptance tests are not implemented or run. Gradle reports
upstream deprecations for a future Gradle 10 upgrade; current pinned builds pass.

Dependency provenance: [Gradle compatibility](https://docs.gradle.org/current/userguide/compatibility.html),
[gRPC releases](https://github.com/grpc/grpc-java/releases),
[protobuf Gradle plugin](https://github.com/google/protobuf-gradle-plugin/releases),
[Adoptium assets](https://api.adoptium.net/v3/assets/latest/21/hotspot?architecture=aarch64&image_type=jdk&os=mac).

## Artifact exclusion follow-up

The owner initialized the repository after the original validation. Generated
JSON/XML evidence, the generated file inventory and binary protobuf baseline were
removed from the staging index; local copies remain ignored. The API baseline now
uses versioned `.proto` source files and generates descriptors only in `build/`.
`./gradlew :protocol:resolveDependencies :protocol:spotlessApply :protocol:test
--write-locks` passed after this change. Original execution records above describe
the initial Q0 validation; this follow-up changed artifact handling only.
