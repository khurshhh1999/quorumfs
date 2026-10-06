# Local ownership and causal versions

Requires JDK 21 and an installed server distribution (`./gradlew :server:installDist`).
Stop the relevant node before using its offline tool; the exclusive RocksDB lock
prevents concurrent server/CLI access. Use the same node configuration each time.

For a fresh example directory, run from the repository root:

```bash
mkdir -p build/causal-demo
sed 's|data.dir=/var/lib/quorumfs|data.dir=build/causal-demo/data|' \
  infra/compose/nodes/node1.properties > build/causal-demo/node1.properties
printf 'chosen content\n' > build/causal-demo/input.txt
server=modules/server/build/install/server/bin/server
config=build/causal-demo/node1.properties
"$server" storage "$config" owners demo greeting
"$server" storage "$config" put-causal demo greeting build/causal-demo/input.txt -
"$server" storage "$config" siblings demo greeting
```

`owners` prints the configuration fingerprint and ordered distinct physical
owners. All five nodes with the same configuration compute the same result.
`put-causal` prints UUID, sequence, size and a hex-encoded causal context. `-`
means empty caller context. To update an observed version, pass its returned
context as the final argument. The program never automatically merges unseen
siblings into a caller's write. `siblings` prints UUID, SHA-256 and context per
visible version; an absent key produces no lines.

To resolve the siblings you just observed, choose content and explicitly pass
their UUIDs, separated by commas. This example captures the current observations:

```bash
observed=$("$server" storage "$config" siblings demo greeting | awk '{print $1}' | paste -sd, -)
"$server" storage "$config" resolve demo greeting build/causal-demo/input.txt "$observed"
"$server" storage "$config" siblings demo greeting
"$server" storage "$config" verify
```

Read chosen or historical bytes with the existing `get` command and an explicit
UUID. Resolution creates a new immutable version. It does not delete history,
and a racing concurrent write may leave another sibling to resolve. Contexts
are bounded to the five configured node IDs and the active set to 32 siblings.
Capacity errors leave the prior committed set unchanged. Equal vectors with
unequal digests are integrity failures, never a choice to overwrite content.
Local success is not a replicated quorum acknowledgment.

## Upgrade, recovery and diagnostics

Back up before upgrade. Existing local-only keys remain readable, but cannot be
mixed with causal writes: copy content into a new key or namespace. Startup adds
a heads column family and persists the exact ring. A direct downgrade to an older
binary is unsupported; use a pre-upgrade backup with its original binary/config.
Checkpoint and restore preserve the current ring, counter, vectors and heads.
Never run the source and a restored copy simultaneously with the same identity.
Do not resume causal writes from a stale checkpoint if later counter values may
exist elsewhere; counter reconciliation is not implemented. Use only a latest
checkpoint taken after stopping writes, with no writes after the checkpoint.

`event=ring_loaded` includes node, epoch, fingerprint and vnode count. Differing
cluster/epoch/membership/quorum configuration fails startup or peer ingestion.
Restore the original configuration; deleting metadata or inventing a new epoch
is not a supported migration. Missing or corrupt causal metadata fails the audit;
preserve the volume and restore a verified compatible checkpoint.

Generated reports live in ignored `build/reports/` and module build directories.
The reproducible local checks are `./gradlew check faultTest` and
`python3 tests/system/storage_disk_full.py` (Docker required).
