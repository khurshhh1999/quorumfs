## PR 4: quorum reads and writes

Enables authenticated development put/get/head/resolve RPCs across the fixed
five-node ring. A successful write requires W validated durable canonical owner
acknowledgments; reads require R valid owner replies before returning data,
conflicts or NOT_FOUND. Coordinator staging never counts toward quorum.

Adds bounded checksummed disk staging and gRPC flow control, preserved replica
UUIDs, idempotent completed-version replay, explicit unknown write outcomes,
namespace/peer authorization and context-aware CLI commands. Slow or invalid
replica replies do not count. Concurrent versions remain visible until explicitly
resolved with observed context.

Validation: 45 unit/property tests, 13 real-network quorum integration tests,
three storage process tests, five-JVM CLI/owner-kill checks, authenticated Docker
Compose checks and native disk-full validation. Full local evidence and commands
are in `docs/evidence/Q3/README.md`. Remote CI at this new head remains pending.

ClusterIdentity adds canonical ring bytes as field 3 without changing existing
fields or RPC numbers. No new RocksDB family/manifest format is added. Upgrade all
five nodes before enabling data methods; back up first and preserve node counters
and identity. Rollback disables data service and uses a compatible pre-upgrade
backup if necessary. Never resume causal writes from stale checkpoints.

Data RPCs are opt-in using separate client/peer credentials plus a namespace
allowlist; absent settings preserve bootstrap mode. Plaintext is still explicit
development-only transport. Deletion, automatic repair, hints, anti-entropy and
mTLS remain later work. No release, deployment or performance claim is included.
Generated artifacts and credentials remain excluded from Git.
