## Behavior

Implement Phase 2 / Q1: durable single-node immutable object storage. An upload is
invisible until all chunks and the object digest verify, staged WAL is synced, and
its manifest plus commit index publish atomically. Local acknowledgment follows
synchronous publication. Public/internal distributed object RPCs stay UNIMPLEMENTED.

## Changes

- Versioned RocksDB column families/keys, persistent identity and non-reused local
  sequence allocation; bounded native resources and upload admission.
- 256 KiB streaming chunks, 64 MiB object limit, strict offsets/lengths, SHA-256
  verification, cancellation/TTL cleanup and fail-closed I/O handling.
- Recovery removes unfinished stages without collecting committed data; audits
  manifest/index/counter integrity and every committed object's content.
- Verified reads, checkpoint creation, validated fresh-directory restore, and an
  offline CLI for put/get/verify/checkpoint/restore with output overwrite protection.
- Real process-kill tests at five publication boundaries, bounded-heap streaming,
  native ENOSPC in Linux tmpfs, corruption/concurrency/backup tests and CLI integration.
- CI runs the native disk-full check. Container packages pin Ubuntu's OpenSSL fix
  for CVE-2026-84782 rather than suppressing the image security gate.

## Validation

See `docs/evidence/Q1/README.md` for commands and results. Generated reports remain
in ignored build directories; no execution artifacts are included in this PR.

24 unit tests and two process tests pass. Process tests cover five kill boundaries
and a 64 MiB object with a 24 MiB Java heap. Native RocksDB reports IOError(NoSpace)
on an 8 MiB filesystem with no acknowledgment. CLI boundary/restart/restore checks,
five-node regression checks, Compose/proxy smoke, formatting, compatibility and
workflow lint pass. The source-only clean build passes with build cache disabled.
Final patched-image HIGH/CRITICAL scan and secret scan report zero findings; SBOM generated locally. Remote CI at the future committed head still needs to run.

## Migration and rollback

Q0 identity-only databases gain additional column families while preserving their
identity. Back up before upgrade. Q0 binaries cannot reopen a Q1-upgraded volume;
rollback means stop the node and restore its pre-Q1 volume. Restore never replaces
an existing destination and requires the exact original identity/configuration.

Use the local CLI only while that node is stopped. Startup now audits all stored
content, so startup time scales with stored bytes. Corruption fails startup and
requires investigation or verified restore; no data is silently discarded.

## Limits

No replication, causal vectors, quorum acknowledgment, deletion, repair or client
security yet. Local sequence numbers are not vector clocks. A failed commit can
have an unknown outcome; reopen and inspect its UUID. Operations serialize per
store instance. Checkpoints are not independent-device backups until copied off
that device. Injected sync failures and process kills do not prove hardware
power-loss durability. No performance claim or benchmark is introduced.
