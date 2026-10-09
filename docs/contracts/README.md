# Versioned API contract

`quorumfs.v1` defines public ObjectStore and internal ReplicaStore contracts.
Authenticated development mode implements public put/get/head/resolve/delete and
internal replicate/read/fetch/apply-tombstone/deliver-hint operations. Merkle and
range exchange remain UNIMPLEMENTED. Without configured access credentials,
only GetClusterInfo and health inspection are enabled.

`v1/` contains the review baseline as `.proto` source files. Gradle compiles both the baseline and current schemas into ignored build directories for comparison. No generated descriptors are committed. The unit compatibility test
requires existing files, packages, options, message/enum types, fields and RPC
signatures to remain unchanged; additive changes are allowed. It checks nested
messages as well. Never regenerate the baseline simply to silence a failure.
Baseline updates require an API review. This conservative gate rejects removal
even when the old field is reserved. In a future major version, reserve removed
field numbers/names, never reuse them, and introduce a new package/service.

The first stream frame is a header; subsequent frames are ordered chunks.
Offsets and sizes use bytes. Digests are raw 32-byte SHA-256 values. Keys are
opaque bytes scoped to a namespace. Missing required semantic fields, unsigned
values exceeding supported limits, duplicate/conflicting chunks and empty
streams are rejected by the RPC handlers. Maximum chunk/object/key sizes:
256 KiB / 64 MiB / 1 KiB.

GetObject emits a version header and verified chunks, or a conflict VersionSet.
A selected version ID retrieves a sibling. Delete and resolution carry observed
causal context. Tombstones and concurrent live writes remain visible as conflicts.

Errors carry ErrorDetail in `quorumfs-error-bin` gRPC trailers:

| Reason | gRPC code |
| --- | --- |
| INVALID_REQUEST | INVALID_ARGUMENT |
| QUORUM_UNAVAILABLE | UNAVAILABLE |
| OUTCOME_UNKNOWN | DEADLINE_EXCEEDED |
| CONFLICT | ABORTED |
| CHECKSUM_MISMATCH | DATA_LOSS |
| CAPACITY_EXHAUSTED | RESOURCE_EXHAUSTED |
| EPOCH_MISMATCH | FAILED_PRECONDITION |
| ACCESS_DENIED | PERMISSION_DENIED |
| OBJECT_NOT_FOUND | NOT_FOUND |

OUTCOME_UNKNOWN sets outcome_unknown=true. A transport timeout can lack trailers
and must also be treated as an unknown write outcome. Request IDs are trace/replay
identifiers, not an exactly-once guarantee. Never blindly retry a timed-out
write without inspecting causal state.

HEAD retains tombstone metadata and context. A tombstone has size zero and the
SHA-256 of empty bytes. GET of a single or selected tombstone returns NOT_FOUND;
multiple maximal heads return conflict metadata. Empty live versions are distinct
from tombstones. ApplyTombstone requires the tombstone bit and canonical owner;
DeliverHint uses identical verified publication and replay rules as ReplicateVersion.
Both require peer authentication and exact full-ring identity. No hint counts as W.

Durable delivery may complete a write after its caller times out. Hint capacity
exhaustion occurs before dispatch and returns RESOURCE_EXHAUSTED. Request IDs do
not provide global deduplication; a new public retry can create a new causal event.
