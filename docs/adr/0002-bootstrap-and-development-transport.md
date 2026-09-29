# ADR 0002: Explicit bootstrap boundaries

Status: accepted for Q0.

Eight Gradle modules establish the planned dependency boundaries. Java 21,
Gradle, protobuf, gRPC, RocksDB and formatting dependencies are pinned. Java
compiler warnings fail handwritten builds; generated sources are compiled too.
Locks and SHA-256 verification metadata accompany dependencies. Dependency hash
updates require review, not an automatic CI refresh.

Health of the empty service name means the bootstrap process has opened its
identity database and bound gRPC. The object service reports NOT_SERVING and
GetClusterInfo explicitly sets object_api_ready=false. Unimplemented public and
internal RPCs retain gRPC UNIMPLEMENTED. Fabricated successful data operations
were rejected because they would conceal the storage work still required.

Plaintext transport requires development.insecure=true and the CLI --insecure
flag. Compose publishes ports only to host loopback. Do not deploy Q0 on an
untrusted network. mTLS, authorization, admission control and object readiness
are later milestones. There is no public metrics endpoint yet; startup logs
record node, cluster, epoch and port without object keys or payloads.
