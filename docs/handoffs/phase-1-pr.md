## Problem and resulting behavior

QuorumFS previously contained only design documents. This change implements the
initial Phase 1 / Q0 bootstrap: a reproducible Java workspace and five real gRPC
nodes with validated fixed membership and durable RocksDB-backed identities.
Nodes expose health and cluster information; object APIs explicitly remain
UNIMPLEMENTED and object-service readiness is false.

## Changes

- Eight Gradle modules; pinned Java 21, Gradle 9.8.0, gRPC 1.84.0, protobuf 4.36.2,
  RocksDB JNI 10.10.1, formatter and JUnit; wrapper/dependency SHA-256 verification
  and dependency locks, including Linux and macOS code-generation artifacts.
- Public/internal v1 protobufs, typed error trailers and additive-only descriptor
  compatibility gate built from versioned `.proto` baseline sources. No previous storage schema or released API to migrate.
- Fail-closed configuration and persisted node/cluster/epoch/membership/quorum
  identity. Invalid R/W/N and duplicate IDs/endpoints fail startup.
- Five non-root, resource-bounded Compose nodes with separate volumes, loopback
  proxy endpoints, real health checks, and a pinned Toxiproxy fault fixture.
- CI gates for formatting/static compilation, unit, integration/contracts,
  process/proxy fault smoke, build, SBOM/security and an always-running aggregate
  gate. Third-party actions are pinned to full commit SHAs.
- Honest preview README, consistency/fault-model ADRs, development recovery
  guidance, evidence and owner-controlled Git/PR handoff.

## Verification

Evidence: `docs/evidence/Q0/README.md` (XML/JSON records are local-only and ignored).

- `./gradlew check assemble faultTest`: passed.
- Source-only clean copy, `./gradlew check assemble faultTest --no-build-cache`:
  passed, 52 tasks executed. This was not a Git checkout; dedicated repo setup
  remains owner-controlled.
- Eight tests passed with zero failures/errors/skips, including all 343 tested
  R/W/N combinations, real RocksDB restart/locking, errors and descriptor checks.
- Five-process integration/restart and process-kill recovery passed with seed
  20260928; changed persisted epoch rejected.
- Compose validation, image build, five health checks, proxy smoke, five distinct
  volumes, and full Compose restart smoke passed.
- Real Toxiproxy disconnect/reconnect test passed.
- Trivy 0.74.0 image HIGH/CRITICAL scan: zero findings; filesystem secret scan:
  zero findings; CycloneDX SBOM generated.
- actionlint passed; shellcheck integration was not run.

Remote GitHub CI has not run at a committed head. Require all checks and owner
review before merge. Local tests ran on macOS ARM64 and Linux ARM64 containers;
Linux x86_64 CI remains pending.

## Operational impact, migration and rollback

Development-only plaintext transport requires explicit opt-in. Published ports
bind to loopback; do not expose this preview publicly. Five persistent volumes
retain identity across restarts. Existing volumes reject changed identity or
membership; restore the original config rather than deleting data to bypass it.

There is no object schema migration. Roll back by stopping the Compose stack and
restoring prior source/configuration. Preserve volumes for diagnosis; `compose
down` does not delete them. No deployment, release, branch-protection changes or
Git publication was performed by the agent.

## Remaining scope

Q1–Q8 remain unimplemented: object streaming/durability, ring selection, vectors,
quorum coordination, tombstones, repair, security hardening and the 10,000-operation
campaign. Bootstrap health/process recovery is not evidence of object durability.
Benchmark intentionally fails until a storage workload exists. Review visibility
of the planning documents before publishing this repository.

Generated test reports, evidence JSON/XML, file inventories, descriptors, packaged
binaries and archives are excluded from Git. Local copies and CI artifact uploads
remain available. Required Gradle wrapper/dependency metadata remain versioned.
The protocol tests passed again after converting the baseline to source files.
