# ADR 0007: Snapshot Merkle anti-entropy

Status: accepted for the development preview.

## Decision

Each authenticated node periodically compares its current causal heads with one
peer. The comparison contains only keys for which both nodes are canonical
owners under the exact same cluster, epoch and full ring configuration. Peer
selection rotates across the other four nodes. The default interval is 15 seconds
after each completed attempt, configurable with the validated JVM property
`quorumfs.antiEntropy.intervalMillis` (1000–300000 milliseconds). No hints or
foreground reads are required to discover divergence.

A RocksDB snapshot captures head entries, manifests, vectors and payload health.
The scan checks chunk and whole-object hashes. A detached immutable inventory is
then retained for up to 60 seconds; the native snapshot is released immediately
after capture. Concurrent subsequent writes require another round and do not
change this view. At most four views are retained, one per requesting peer.
A new request replaces that peer's previous view. Views and cursors are not
valid after restart or expiry; the next round starts from a new snapshot.

A fixed binary Merkle tree has 16 leaves spanning the unsigned SHA-256/128 token
space. Canonical entry order is length-safe namespace/key/UUID identity order.
Leaf hashes include key, UUID, canonical sorted vector, size, digest, tombstone
bit and verified payload-health bit; parent hashes are domain-separated from
leaves. Root and leaf ranges are identical on both sides. The full-space range
has all-ones start and end boundaries; other ranges use `(start, end]` semantics.
The worker compares roots, descends four levels and requests only divergent
leaves. It verifies paginated metadata against the advertised leaf hash before
transferring any versions from that leaf.

Range exchange streams at most 128 metadata entries per page. Received entries
are capped at 4096 serialized bytes each and 8 MiB per collected leaf, including
unknown protobuf fields; a peer cannot bypass the budget with opaque fields. A continuation
contains snapshot UUID, leaf index and offset and is bound to the requesting
peer's view. Invalid ranges, foreign/expired snapshots, unsigned overflows,
foreign-owner entries and invalid identities are rejected. The worker merges
maximal causal versions from both views, preserving concurrent siblings and
retained tombstones. Only a healthy source can supply a replacement. Transfers
use the existing chunk/whole-object verification, owner authorization, identity
checks and durable acknowledgment path.

Each node runs one anti-entropy worker. An inventory is bounded by 8192 shared
versions, 4 MiB of accounted metadata, 512 MiB of payload verification and 65536
scanned head records. Exceeding a limit fails the comparison with capacity
exhaustion; it never publishes a truncated tree or claims equality. A round has
a 45-second RPC budget, at most 128 version transfer attempts and 128 MiB of
attempted payload transfers; individual payload RPCs retain the three-second
cap. Peer, leaf and entry cursors prevent a large or repeatedly failing range
from permanently starving later work. Committed replicas are durable progress;
restart discards only transient comparison state and safely retries.

## Corruption repair and storage format

Matching metadata does not prove that payload bytes are intact. The health bit
makes a healthy/corrupt pair diverge. When an existing UUID's immutable metadata
matches a verified incoming version but its chunks are damaged, repair writes
replacement chunks under a fresh reserved payload UUID. After complete checksum
validation and WAL synchronization, an atomic batch switches the manifest's
payload pointer and removes staging. It never overwrites the old chunk set.
A crash before publication keeps the old pointer; after publication it keeps the
complete new pointer. Older payloads remain retained. Invalid replacement bytes
cannot change the committed version or its causal identity.

Storage marker 3 accepts and upgrades markers 1 and 2. Manifest encoding 4 adds a
16-byte payload UUID to the existing live causal manifest; encodings 1–3 remain
readable. Reserved payload UUIDs use prefix 2 in the existing request-index family
and cannot be reused as version IDs. No column families or dependencies are added.
Checkpoint/restore audits and preserves the pointer, reservations, replacement
chunks, causal heads, tombstones and hints together. Previous binaries reject
marker 3, so all nodes require a coordinated maintenance upgrade with backups.
Mixed-version service and in-place downgrade are unsupported.

## Limits

Equality is evidence about two captured views, not a global convergence barrier
or proof about writes after capture. Views retain maximal heads; historical
versions stay local and are not all broadcast. There is no tombstone or history
GC. Native RocksDB corruption, corrupt metadata, or corruption discovered during
startup still fails closed and requires operator recovery. Online anti-entropy
repairs detectable missing/truncated/checksum-invalid object chunks while an
intact canonical copy exists. If every copy is corrupt or lost, it cannot invent
valid data. Pairwise snapshots exceeding the preview bounds need operator
intervention; they are not silently split or skipped. Throughput and the later
large fault campaign remain separate work.
