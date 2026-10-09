# Quorum reads, writes and recovery

## Enable the local fixture

Use JDK 21, Python 3 and Docker Compose. From the repository root:

```bash
./gradlew :server:installDist :client:installDist
export QUORUMFS_CLIENT_TOKEN=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
export QUORUMFS_PEER_TOKEN=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
export QUORUMFS_NAMESPACES=demo
docker compose -f infra/compose/compose.yaml up --build --wait --wait-timeout 180
client=modules/client/build/install/client/bin/client
"$client" info localhost:19001 --insecure
```

The final field is `true` when data RPCs are enabled. Missing all three settings
keeps bootstrap mode (`false`); partial settings fail startup. Use separate
credentials and distribute identical settings to the fixed five nodes. Never
check credentials into Git. This preview uses plaintext transport explicitly;
keep it on the trusted loopback fixture until mTLS is implemented.

## Write and read

```bash
work=$(mktemp -d)
key=$(python3 -c 'import uuid; print(uuid.uuid4())')
printf 'hello quorumfs\n' > "$work/input.txt"
"$client" owners localhost:19001 --insecure demo "$key"
"$client" put localhost:19001 --insecure demo "$key" "$work/input.txt"
"$client" head localhost:19002 --insecure demo "$key"
"$client" get localhost:19003 --insecure demo "$key" "$work/output.txt"
cmp "$work/input.txt" "$work/output.txt"
```

Metadata lines contain UUID, size, SHA-256 hex, causal-context hex and a
`live`/`tombstone` kind. A successful
write also prints `durable_owners=2` with default N=3/R=2/W=2. The coordinator may
be any node, but only canonical owners count. Existing download paths are never
overwritten. Downloads verify the entire payload before publishing the file.

## Update or resolve observed siblings

```bash
context=$("$client" context localhost:19002 --insecure demo "$key")
printf 'chosen replacement\n' > "$work/chosen.txt"
"$client" resolve localhost:19004 --insecure demo "$key" "$work/chosen.txt" "$context"
"$client" head localhost:19005 --insecure demo "$key"
```

`context` merges precisely the versions visible to that quorum observation.
`put` also accepts context as its optional final argument for causal updates.
Omitting context (or passing `-`) is a blind write and can create siblings.
A read without a selected UUID reports a conflict when siblings remain; inspect
`head`, then fetch an explicitly selected visible UUID as the final `get` argument
or resolve with chosen bytes and merged context. Racing unobserved writes can
remain as siblings. Superseded versions are retained locally but are not visible
quorum heads; use offline tools for historical UUIDs.

## Failure interpretation

- UNAVAILABLE before dispatch: no quorum could be established; do not reduce R/W.
- DEADLINE_EXCEEDED or interrupted write: outcome unknown. Some owners may have
  committed. Query `head`/`context` after connectivity returns before retrying.
- NOT_FOUND: obtained only after R valid owner responses, never a single miss.
- ABORTED/conflict: inspect sibling metadata and explicitly select/resolve.
- DATA_LOSS: metadata/content integrity failure; preserve the affected volume.
- RESOURCE_EXHAUSTED: operation slots, sibling limits or the durable hint budget; resolve/retry after
  capacity is available. Never delete an incomparable version to make space.
- PERMISSION_DENIED: check client credential and allowed namespace. Peer and
  client credentials are not interchangeable.
- FAILED_PRECONDITION: restore matching cluster/epoch/ring configuration.

The client default timeout is 10 seconds. `QUORUMFS_TIMEOUT_MS` accepts >0..15000;
server uploads are capped at 15 seconds and each peer call at 3 seconds. A slow
third owner does not delay W valid acknowledgments. Cancellation can leave a
committed replica; it never converts a partial write into confirmed success.

Use `./gradlew check faultTest`, `python3 tests/system/storage_disk_full.py`, and
`python3 tests/system/quorum_compose.py` for reproducible local validation. The
Compose test creates ephemeral credentials and shuts its fixture down.

## Limits and shutdown

Eight public and eight replica operations per node, 256 KiB chunks, 64 MiB objects,
32 siblings and five fixed vector identities. Disk staging can use up to 1 GiB
plus native upload staging and committed data. Recovery adds up to 192 MiB of
temporary transfers and a 256 MiB/128-version durable queue. Storage retains history; scans
and startup auditing grow with it. No throughput or power-loss guarantee is made.

```bash
docker compose -f infra/compose/compose.yaml down
```

Volumes survive shutdown. Acknowledged data survives tested owner process kills
and volume-preserving restarts. Durable hints and read repair now propagate live
versions and tombstones. See [deletion and recovery](recovery.md) for semantics,
backlog handling and the coordinated storage-format upgrade. Anti-entropy is
still pending.
Back up before upgrade; do not resume writes from stale checkpoints or run two
copies of a node identity. Upgrade all nodes together before enabling these RPCs.
