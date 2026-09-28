# Development bootstrap and recovery

Use JDK 21 (pinned CI distribution: Temurin 21.0.12.1+1), Python 3 and Docker
Compose. Gradle does not download a JDK. Set JAVA_HOME to the installed JDK 21.
On this development Mac, the verified task-local JDK is
`.tools/jdk-21.0.12.1+1/Contents/Home`; it is ignored and is not a distributable.

`./gradlew check assemble faultTest` builds and verifies the bootstrap.
`./gradlew integrationTest` starts five real Java processes with ephemeral ports
and temporary independent RocksDB directories, checks health and identity,
restarts all processes, and rejects a changed epoch. `faultTest` additionally
kills a process, confirms failure to reach it, checks the remaining processes,
and restarts with the original identity. It does not validate object durability
or quorum availability. Reports live under `build/reports/system`.

`./gradlew smokeTest` expects running endpoints on loopback ports 19001–19005;
set QUORUMFS_PORTS to five comma-separated alternate ports if needed. It does
not silently start an alternative cluster. `benchmark` intentionally fails with
an explicit not-implemented error while no storage workload exists.

Startup rejects unknown/missing configuration keys, non-five-node membership,
duplicate node IDs or endpoints, invalid ports/epochs, invalid R/W/N, an absent
local ID, and changed persisted identity. Stop the conflicting process if RocksDB
reports a volume lock. Do not delete a volume to bypass an identity mismatch.
Restore the original configuration, or provision a separate empty development
volume with a new identity; retain the old volume for investigation.

Stop the local Compose stack with `docker compose -f infra/compose/compose.yaml down`.
This preserves the named volumes. Restarting with the same configuration reopens
the identity database. There is no object backup/restore or storage-format
migration in Q0. Rollback means stop this bootstrap and restore the prior source
and configuration; keep volumes for diagnosis. No release/deployment is authorized
or configured by this change.

Dependency updates: change explicit pins, run `./gradlew resolveDependencies
--write-locks --write-verification-metadata sha256`, then formatting/check/build
and both platform builds. Review every dependency/hash change. Never disable
verification to pass a normal build. The descriptor baseline is updated only
with explicit contract review.

## Compose fault fixture

All external node traffic goes through Toxiproxy. Run
`python3 tests/system/proxy_fault.py` after starting the stack to disable node1's
proxy, verify node1 is unreachable and node2 remains healthy, restore the proxy
in a finally block, and verify reconnection. This exercises transport faults,
not replica quorum behavior (no replication exists yet). Never expose the proxy
admin endpoint publicly. Compose uses an executable, size-limited temporary
mount because RocksDB JNI extracts and loads its native library there; the
container root filesystem remains read-only and the process runs as UID 10001.
