# QuorumFS

QuorumFS is a developer-preview distributed object store with five fixed nodes,
consistent hashing, durable RocksDB storage and configurable strict read/write
quorums. It supports authenticated local-development uploads, verified downloads,
causal sibling inspection and explicit conflict resolution. Deletion and automatic
replica repair are not implemented yet.

## Run locally

Requires JDK 21, Python 3 and Docker with Compose. Set `JAVA_HOME` to JDK 21.
The checked-in Gradle wrapper verifies pinned build dependencies.

```bash
./gradlew check assemble faultTest
./gradlew :server:installDist :client:installDist
export QUORUMFS_CLIENT_TOKEN=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
export QUORUMFS_PEER_TOKEN=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
export QUORUMFS_NAMESPACES=demo
docker compose -f infra/compose/compose.yaml up --build --wait --wait-timeout 180
client=modules/client/build/install/client/bin/client
"$client" info localhost:19001 --insecure
```

Info reports node ID, cluster ID, epoch, member count, N/R/W and object readiness.
The final field is `true` with valid credentials. Without all three environment
settings, the server retains inspection-only bootstrap mode; partial credential
configuration fails startup. Credentials are separate for public and peer calls.
Client access is restricted to the namespace allowlist.

Ports 19001–19005 reach the nodes through the fault proxy. Port 18474 is its
administration endpoint. Published ports bind to loopback. Transport remains
plaintext with explicit development opt-in; keep this fixture on a trusted local
machine. mTLS and individual client policies are not implemented yet.

## Upload, download and resolve

```bash
work=$(mktemp -d)
key=$(python3 -c 'import uuid; print(uuid.uuid4())')
printf 'hello quorumfs\n' > "$work/input.txt"
"$client" put localhost:19001 --insecure demo "$key" "$work/input.txt"
"$client" head localhost:19002 --insecure demo "$key"
"$client" get localhost:19003 --insecure demo "$key" "$work/output.txt"
cmp "$work/input.txt" "$work/output.txt"
context=$("$client" context localhost:19002 --insecure demo "$key")
printf 'chosen replacement\n' > "$work/chosen.txt"
"$client" resolve localhost:19004 --insecure demo "$key" "$work/chosen.txt" "$context"
"$client" head localhost:19005 --insecure demo "$key"
```

Metadata lines contain UUID, size, SHA-256 and causal-context hex. A successful
write additionally prints its durable owner count. `put` accepts an optional
context argument for causal updates. A context-free write can create concurrent
siblings. `get` reports a conflict when several visible versions remain; use
`head` and append a chosen UUID to `get`, or explicitly resolve observed context.
A racing unobserved version may remain a sibling. Existing output paths are
never overwritten, and downloads are published only after checksum verification.

Default N=3/R=2/W=2. Success requires W complete durable canonical owners;
reads require R valid owner replies. Coordinator staging and non-owner copies
never count. Timeouts can have an **unknown write outcome**: inspect state and
context before retrying. Quorum overlap does not imply linearizability or
exactly-once requests. See the [quorum runbook](docs/runbooks/quorum.md).

```bash
"$client" health localhost:19001 --insecure
docker compose -f infra/compose/compose.yaml down
```

Stopping preserves volumes. Health/readiness describe local service startup;
quorum availability is checked per operation. A returning stale replica does
not catch up automatically yet: hinted handoff, read repair and anti-entropy are
still absent. Delete RPCs remain `UNIMPLEMENTED`.

## Local storage and configuration

The offline tool supports local put/get/verify/checkpoint/restore plus ring and
causal-version inspection. Stop the node before opening its data directory.
See [storage and backups](docs/runbooks/storage.md) and the
[offline causal walkthrough](docs/runbooks/causal-versions.md).

Exactly five unique fixed members are required. Quorums must satisfy
`1 <= R,W <= N <= 5` and `R + W > N`. Reusing a volume with changed cluster ID,
epoch, node ID, membership or quorum fails startup. Never erase identity to
bypass this check. All nodes must use the same configuration and compatible
binary before enabling data RPCs. Back up before upgrading; do not originate
writes from stale checkpoints or run two copies of one node identity.

Objects are at most 64 MiB, streamed in 256 KiB chunks. Each node admits eight
public and eight replica operations, with at most 32 visible siblings per key.
Temporary disk staging can use up to 1 GiB plus native upload staging and retained
object history. The client defaults to a 10-second deadline, server uploads to a
15-second maximum, and individual peer RPCs to 3 seconds. Slow transfers can time
out. There is no throughput or hardware power-loss claim.

## Troubleshooting and support

Use Compose logs for startup errors. Check `JAVA_HOME` if JDK 21 cannot be found.
A RocksDB lock error means another process is using the data directory. Restore
the original configuration for identity errors; verify credentials and namespace
permissions for access errors. Preserve corrupt volumes and restore only a
verified compatible checkpoint. See the [development runbook](docs/runbooks/development.md).

Report reproducible issues with runtime versions and sanitized logs. This is an
object store, not a mounted filesystem, POSIX implementation or S3 endpoint.
Production deployment and support are not available.
