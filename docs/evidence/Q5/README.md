# Phase 6 / Q5 local evidence

Date: 2026-10-11 (America/New_York). Branch `pr-6` starts at `origin/main`
`50269b8`, with Phase 5 merged as GitHub PR #9. The owner retains publication;
remote CI at the eventual Phase 6 commit remains pending.

Environment: macOS ARM64, Temurin 21.0.12.1+1, Gradle 9.8.0, RocksDB JNI
10.10.1, Docker Engine 29.4.0 and Trivy 0.74.0. No dependency changes.

## Acceptance evidence

57 unit/property tests, 23 five-node network tests and eight storage process
tests pass with zero failures/errors/skips: 88 total. All previous quorum,
authorization, wire compatibility, causal, deletion, hint and storage tests remain.
New tests cover:

- Merkle permutation invariance across deterministic seeds, all fixed levels and
  changes in vector, deletion and verified payload-health metadata.
- Snapshot pagination across 130 entries, mutation after capture, stable old view,
  new-view divergence, expiry and rejection of foreign cursors, peers, ranges,
  limits and epochs. Retained views never claim to include subsequent mutations.
- Real TCP partition and node restart, followed by automatic convergence without
  any hints or foreground reads. Maximal live/tombstone/concurrent siblings all
  reach the canonical owners; stale predecessors remain superseded.
- Online native RocksDB value corruption beneath matching immutable metadata:
  the Merkle health bit differs and a verified peer copy repairs the payload.
  Version identity survives repair, restart and checkpoint restore.
- A 140-version divergent leaf spans metadata pages and the 128-version transfer
  budget. The first round publishes 128, and the next publishes the remaining 12.
- Oversized peer metadata, including unknown protobuf fields, fails before local
  publication; entry-count bounds cannot bypass metadata byte limits.
- Invalid replacement data leaves existing committed bytes intact. Reserved
  payload identities cannot be reused as replicated version UUIDs.
- Five actual JVM kills around repair chunk writes, WAL sync, manifest publication,
  post-publication and acknowledgment; old or complete replacement bytes survive.
  With earlier fixtures, the suite now covers 24 storage kill boundaries.
- A 64 MiB copy-on-write repair under a 24 MiB Java heap. Existing maximum-object
  and 256 MiB pinned-hint budget heap tests remain passing.

New deterministic payload/tree seed: 20261011. Existing storage process seed:
20260930; older ring/bootstrap and quorum seeds remain unchanged.

## Reproduce

Use JDK 21 in `JAVA_HOME`:

```bash
./gradlew spotlessApply check faultTest assemble :client:installDist
python3 tests/system/storage_disk_full.py
python3 tests/system/quorum_compose.py
```

## Local gates

- Working-tree `spotlessApply check faultTest assemble :client:installDist` passed;
  after the final metadata guard, `spotlessApply :server:quorumIntegrationTest
  :server:installDist :client:installDist` passed with all 23 network tests.
- Final source-only clean build in `.tools/pr-6-final-clean` passed
  `--no-build-cache check faultTest assemble :client:installDist`: all 70 tasks
  executed in 3m 29s, all 88 Java tests passed, and the real-process CLI/bootstrap/
  storage/fault fixtures passed. Code, tests and build inputs match the worktree.
  The existing Gradle 10 deprecation warning remains; no gate was disabled.
- Native RocksDB disk-full fixture passed on an 8 MiB tmpfs with an actual native
  I/O error and no durable acknowledgment.
- Final five-container authenticated CLI passed PUT, read, resolution, delete and
  recreation. Bootstrap smoke and proxy disconnect/reconnect also passed (the
  proxy fixture itself is not object-quorum evidence). Containers were stopped;
  named volumes were preserved.
- A real Phase 5 binary wrote format-2 causal data; the new binary upgraded it and
  read identical bytes. The old binary then rejected format 3, as documented.
- Trivy scanned the final container image: zero HIGH/CRITICAL vulnerabilities.
  Source secret scan reported zero findings; CycloneDX SBOM generated locally.
- Actionlint, Compose configuration validation and `git diff --check` passed.
- Exact 24-file staging dry run passed. The concise handoff staging command selects
  the same files. The index is empty; no ignored files are tracked, and all four
  local planning/instruction files are ignored and untracked.

Local logs/reports: `.tools/pr-6-final-clean-check.log`,
`build/phase6-final-network.log`, `build/phase6-final-compose.log`,
`build/phase6-smoke.log`, `build/reports/phase6-upgrade.json`,
`build/reports/phase6-image-security.json`, `build/reports/phase6-secrets.json`,
`build/reports/phase6-sbom.cdx.json` and `build/reports/system/`.
The exact changed-file list and owner commands are in
`docs/handoffs/phase-6-commands.md`; the PR body is `docs/handoffs/phase-6-pr.md`.

## Limits

The clean build is a source-only copy, not a remote CI execution. Raw logs, XML,
JSON, SBOM and databases remain in ignored build/tool directories. Planning and
agent-instruction files remain ignored and untracked.

This phase tests the declared bounded process/partition model; it is not the
later 10,000-operation campaign, throughput evidence or a hardware power-loss
claim. Pairwise snapshots have explicit count/metadata/scan-byte/head-scan limits;
exceeding them fails instead of claiming equality. Snapshot agreement is not a
barrier for subsequent writes. Repair needs a surviving valid canonical copy.
Startup, metadata and native RocksDB corruption still fail closed for operator
recovery. Old chunks, versions and tombstones remain retained indefinitely.
See ADR 0007 and `docs/runbooks/anti-entropy.md` for format-3 migration and rollback.
