# QuorumFS

QuorumFS is a developer-preview foundation for a distributed object store with
configurable replica and quorum settings. The current build runs five gRPC nodes,
validates fixed membership, persists node identities in RocksDB, and provides
health and cluster-information commands. **Object storage operations are not
implemented yet** and return `UNIMPLEMENTED`.

## Run locally

Requires JDK 21, Python 3, and Docker with Compose. Set `JAVA_HOME` to your JDK 21
installation. The build uses the checked-in, checksum-verified Gradle wrapper.

```bash
./gradlew check assemble faultTest
./gradlew :server:installDist :client:installDist
docker compose -f infra/compose/compose.yaml up --build --wait --wait-timeout 180
./gradlew smokeTest
modules/client/build/install/client/bin/client info localhost:19001 --insecure
```

The info command reports `node1 quorumfs-dev 1 5 3 2 2 false`: node ID, cluster ID,
epoch, member count, replication factor, read quorum, write quorum, and object-API
readiness. All five nodes must have distinct IDs. Ports 19001–19005 reach the nodes
through the local fault proxy. Port 18474 is the proxy administration endpoint.
Published ports bind to loopback only.

```bash
modules/client/build/install/client/bin/client health localhost:19001 --insecure
docker compose -f infra/compose/compose.yaml down
```

Stopping Compose preserves each node's independent named volume. Restarting with
the same configuration preserves its identity. `health` means the bootstrap
process and identity database are available; it does not mean object operations
are ready. The object service's named health check remains `NOT_SERVING`.

## Configuration and limits

Node files live in `infra/compose/nodes/`. Exactly five unique members are required.
Defaults are N=3, R=2, W=2; configuration must satisfy `1 <= R,W <= N <= 5` and
`R + W > N`. Changing a persisted cluster ID, epoch, node ID, membership or quorum
configuration causes startup to fail. Do not erase a volume to bypass this check.

Transport is plaintext and requires explicit development-mode opt-in. Do not expose
this preview to untrusted networks. There is no authentication, object durability,
replication, causal conflict handling or repair yet. This is an object-store project,
not a mounted filesystem, POSIX implementation or S3-compatible endpoint.

## Troubleshooting and support

Use `docker compose -f infra/compose/compose.yaml logs` for startup errors. Check
`JAVA_HOME` if the build cannot find JDK 21. A RocksDB lock error means another
process is using the same data directory. A persisted-identity error requires
restoring the original configuration. See [development recovery guidance](docs/runbooks/development.md).

Report reproducible issues with runtime versions and sanitized startup logs.
Production deployment and support are not available.
