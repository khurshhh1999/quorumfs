package io.quorumfs.protocol;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quorumfs.protocol.v1.ErrorDetail;
import io.quorumfs.protocol.v1.ErrorReason;

public final class Errors {
  private Errors() {}

  public static final Metadata.Key<byte[]> DETAIL =
      Metadata.Key.of("quorumfs-error-bin", Metadata.BINARY_BYTE_MARSHALLER);

  public static StatusRuntimeException exception(ErrorReason reason) {
    Status status =
        switch (reason) {
          case INVALID_REQUEST -> Status.INVALID_ARGUMENT;
          case QUORUM_UNAVAILABLE -> Status.UNAVAILABLE;
          case OUTCOME_UNKNOWN -> Status.DEADLINE_EXCEEDED;
          case CONFLICT -> Status.ABORTED;
          case CHECKSUM_MISMATCH -> Status.DATA_LOSS;
          case CAPACITY_EXHAUSTED -> Status.RESOURCE_EXHAUSTED;
          case EPOCH_MISMATCH -> Status.FAILED_PRECONDITION;
          case ACCESS_DENIED -> Status.PERMISSION_DENIED;
          case OBJECT_NOT_FOUND -> Status.NOT_FOUND;
          default -> throw new IllegalArgumentException("Explicit error reason required");
        };
    Metadata trailers = new Metadata();
    trailers.put(
        DETAIL,
        ErrorDetail.newBuilder()
            .setReason(reason)
            .setOutcomeUnknown(reason == ErrorReason.OUTCOME_UNKNOWN)
            .build()
            .toByteArray());
    return status.withDescription(reason.name()).asRuntimeException(trailers);
  }
}
