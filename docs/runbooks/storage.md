# Local storage and recovery

This tool is offline and exclusively locks the configured node directory. Stop
that node first. It does not contact replicas or issue a quorum acknowledgment.
Build with JDK 21: `./gradlew :server:installDist`.

Entry point: `modules/server/build/install/server/bin/server storage CONFIG COMMAND`.
CONFIG has the same validated properties and stable identity as the node server.

| Command arguments | Result |
| --- | --- |
| `put NAMESPACE KEY INPUT_FILE` | UUID, local sequence, size; immutable version |
| `get NAMESPACE KEY UUID OUTPUT_FILE` | Verified output; refuses existing output |
| `verify` | Verify all committed versions and report count |
| `checkpoint NEW_DIRECTORY` | Consistent checkpoint after integrity verification |
| `restore CHECKPOINT_DIRECTORY` | Restore into CONFIG's data.dir, which must not exist |

Use the returned UUID for reads. There is no latest-version selection, vector
merge, overwrite, deletion, distributed retry deduplication, or RPC storage yet.
Keys in this CLI are UTF-8; the library supports binary keys. Existing immutable
versions remain accessible after subsequent writes to the same key.

## Failure and cancellation

INVALID: correct the key, size, checksum or frame order; the rejected upload is
cancelled. CAPACITY: finish/cancel uploads or wait for the configured TTL.
CANCELLED: the upload was closed, interrupted or expired; begin a new upload.
NOT_FOUND: no committed local version exists under that namespace/key/UUID.
CORRUPT: preserve the volume and investigate; never serve unchecked data or erase
the database to hide corruption. IO_FAILURE: no success acknowledgment is issued,
but commit outcome can be unknown. Close the failed store, resolve the underlying
storage issue, reopen, and inspect the original UUID before retrying. CLOSED:
open a new instance. These are local library codes, not new public RPC semantics.

Disk full: stop writes, retain the data directory, expand/free unrelated disk
space, then restart. Recovery needs free space to persist staging cleanup. Never
remove WAL, SST, CURRENT or manifest files manually. A sync error also requires
repair of the underlying filesystem/device before reopening. Automatic retries
on the same failed instance are rejected.

The library exposes bounded counters through `stats()` (active uploads, commits,
abort/recovery count, detected read corruption, I/O failures and failed state).
Startup logs include the node ID and recovered-upload count. Exported monitoring
and repair remain later milestones. Startup verifies all object content and is
therefore proportional to stored bytes; an audit failure prevents readiness.

## Backup and restore

1. Finish/cancel active uploads. Run `checkpoint` to a new directory outside the
   database, then copy that checkpoint to independent storage if needed. A local
   checkpoint can share hardlinks and does not protect against device destruction.
2. Prepare a configuration with the original node/cluster/epoch/membership/quorums
   and a fresh data.dir. A different data.dir does not change the identity.
3. Run `restore CHECKPOINT_DIRECTORY`, then `verify` and read known UUIDs.
4. Use only one active instance of a node identity. Keep the old directory for
   diagnosis until recovery is reviewed. Restore rejects an existing destination.

Q0 -> Q1 adds column families while preserving identity. Take a copy/checkpoint
before upgrading. To roll back to Q0, stop the node and restore its pre-Q1 volume.
Do not open the upgraded directory with the old binary. The storage schema is
versioned, and unknown versions fail closed. Q1 has no distributed rolling-upgrade
or automatic repair guarantee.

## Reproduce checks

`./gradlew check assemble faultTest` runs unit, CLI, five-process and storage
process-recovery checks. `python3 tests/system/storage_disk_full.py` additionally
requires Docker and the classes/distribution built by that command. It uses a
throwaway 8 MiB tmpfs and verifies native NoSpace with no acknowledgment.

Generated reports live under ignored build directories. No reports, archives,
descriptor binaries, DB files or other execution artifacts belong in Git.

## Recovery format upgrade

The deletion/recovery binary accepts prior format 1 databases and synchronously
upgrades the marker to 2. It retains legacy/live manifests and adds causal
tombstones and bounded pinned hint payloads. Old binaries reject format 2.
Checkpoints include this recovery state; startup/restore verifies published hint
checksums and removes only unpublished hint chunks. Stop all nodes and back up
before upgrading; mixed-version service and in-place downgrade are unsupported.
See [deletion and recovery](recovery.md) for rollback and backlog procedures.
