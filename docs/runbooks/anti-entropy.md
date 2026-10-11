# Periodic anti-entropy and corruption recovery

Authenticated development nodes automatically start a single Merkle worker.
Each attempt selects one of the other four nodes, comparing only keys they both
own. It can recover missing versions and tombstones even when hints are absent
and clients are idle. The default delay between attempts is 15 seconds plus
work time; a full healthy peer rotation normally takes at least one minute.

For a standalone server, set a different interval before launch:

```bash
export JAVA_OPTS="${JAVA_OPTS:-} -Dquorumfs.antiEntropy.intervalMillis=5000"
```

Valid intervals are 1000–300000 milliseconds. An invalid value fails startup.
For Compose, retain its existing heap/direct-memory settings when adding the
property to the `JAVA_OPTS` environment entry. Lower intervals increase scanning
and disk load. There is one worker and no unbounded queue.

## Observe and recover

- `event=anti_entropy_equal`: two snapshot roots agree. `corrupt_versions` counts
  locally detected damaged versions in that comparison. Equality with nonzero
  corruption is not proof of healthy data.
- `event=anti_entropy_round`: divergent leaf count, repaired versions, failed
  transfers and attempted bytes. Later writes are checked in a subsequent round.
- `event=anti_entropy_budget`: bounded progress stopped at the transfer budget;
  later attempts resume fairly. Do not lower quorum to accelerate convergence.
- `event=anti_entropy_retry`: peer and status. Check membership, credentials,
  reachability, disk state and capacity before restarting or changing settings.

A snapshot allows 8192 shared versions, 4 MiB of accounted metadata, 512 MiB of
payload verification and 65536 scanned head records. Larger inventories return
RESOURCE_EXHAUSTED and do not claim equality. This preview has no automatic
subdivision beyond those limits. A round attempts at most 128 versions/128 MiB,
uses a 45-second RPC budget, and retains three-second payload RPC deadlines.
Snapshots expire after 60 seconds, with one per peer and four total. Cursors
cannot be reused for another range, peer, snapshot or restarted node.

After a partition, restore connectivity and the original node identity/config.
Allow multiple peer rotations; inspect round/retry logs and validate canonical
replicas with the deterministic integration fixture. Do not infer global
convergence from one equality log. New mutations can require another pass.
Partial rounds and process restarts are safe because each completed transfer is
durably published and repeat delivery is idempotent.

For online missing/truncated/checksum-invalid object chunks, a healthy owner's
snapshot differs even if vector/digest metadata matches. The worker fetches a
verified copy and atomically publishes replacement chunks under the same version
identity. Readers never receive unverified bytes. Corrupt metadata or native
RocksDB files, or corruption found during startup audit, still prevents service:
preserve the damaged volume and restore a verified compatible recovery point.
Do not bypass the audit or remove causal metadata to force startup. Repair needs
an intact canonical copy; total loss cannot be repaired.

## Upgrade and storage

Stop all five nodes and take verified backups before upgrading. The new binary
upgrades storage markers 1/2 to 3. Old binaries refuse marker 3. Replacement chunks
and payload reservations are included in checkpoint/restore. Repair retains old
chunks and all tombstones/history, so disk usage can grow even after convergence.
One additional 64 MiB transfer spool can be used by anti-entropy; native staging,
WAL/compaction and retained data need further headroom.

In-place downgrade and mixed-version service are unsupported. Rollback requires
a verified whole-cluster recovery point and reconciliation of later acknowledged
writes; never originate new writes from stale single-node counters. See
[storage](storage.md), [recovery](recovery.md) and ADR 0007.

## Verify

```bash
./gradlew check faultTest assemble
```

The suite includes real five-node TCP partitions/restarts without hints or reads,
concurrent siblings and deletion, live payload corruption, snapshot mutation and
expiry, paginated exchange, transfer-budget continuation, actual JVM kills around
repair publication, and a maximum-size repair under a 24 MiB heap. Generated
reports remain in ignored build directories.
