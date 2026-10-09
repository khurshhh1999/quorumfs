## PR 5: deletion and recovery

Adds causal DELETE, durable hinted handoff and bounded read repair. An offline
canonical owner receives retained tombstones after reconnecting; a concurrent
live write stays a conflict. HEAD exposes deletion context for recreation, and
GET never publishes tombstones as empty files. The CLI adds `delete` and a
live/tombstone metadata field.

Before remote dispatch, writes durably capture one checksummed payload and all
canonical delivery intents. Only W validated owner acknowledgments count toward
success. Remaining work survives coordinator restart/checkpoint, uses bounded
backoff, and is removed only after durable acknowledgment. Full queues reject
writes before publication. Repair shares the idempotent replica path and
preserves causal siblings; discovery has bounded admission and accepted payloads
use durable retry.

Validation: 53 unit/property, 18 network and six process tests (77 total), plus a
clean 68-task build with no build cache, Docker smoke, native disk-full and zero
HIGH/CRITICAL image or secret findings. Reproducible commands are in
`docs/evidence/Q4/README.md`. Includes real
five-node TCP partition/restart tests, separate JVM crash boundaries for tombstone
repair and hint publication/acknowledgment, bounded-heap hint capacity checks,
CLI deletion/recreation and Docker/native disk-full checks. Remote CI for the
owner's eventual commit remains pending.

Storage marker upgrades from 1 to 2; legacy/live data stays readable and a new
manifest kind records tombstones. No new dependencies or protobuf field changes.
Back up and stop all five nodes for coordinated upgrade. Old binaries reject
format 2; in-place downgrade and mixed-version service are unsupported. Recovery
and rollback requirements are in ADR 0006 and `docs/runbooks/recovery.md`.

Tombstones/history and unacknowledged hints have no time-based expiration.
The per-node hint budget is 128 versions/256 MiB; recovery uses one delivery worker
and two read-repair preparation slots. Backlog/retry/deferred-repair logs are
available. Anti-entropy, mTLS, operational hardening and the large fault campaign
remain later phases. No deployment, release or performance guarantee is included.
Local planning/agent files and generated artifacts are ignored and excluded from
the staging commands.
