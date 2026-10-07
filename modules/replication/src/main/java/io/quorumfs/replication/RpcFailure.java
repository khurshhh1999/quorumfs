package io.quorumfs.replication;

import io.grpc.*;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.storage.StorageException;

public final class RpcFailure {
  private RpcFailure() {}

  public static RuntimeException map(Throwable error) {
    while ((error instanceof java.util.concurrent.ExecutionException
            || error instanceof java.util.concurrent.CompletionException)
        && error.getCause() != null) error = error.getCause();
    if (error instanceof StatusRuntimeException status) return status;
    if (error instanceof StorageException storage)
      return Errors.exception(
          switch (storage.code()) {
            case INVALID -> ErrorReason.INVALID_REQUEST;
            case CAPACITY -> ErrorReason.CAPACITY_EXHAUSTED;
            case CORRUPT -> ErrorReason.CHECKSUM_MISMATCH;
            case NOT_FOUND -> ErrorReason.OBJECT_NOT_FOUND;
            case CANCELLED -> ErrorReason.OUTCOME_UNKNOWN;
            default -> ErrorReason.QUORUM_UNAVAILABLE;
          });
    if (error instanceof IllegalArgumentException)
      return Errors.exception(ErrorReason.INVALID_REQUEST);
    if (error instanceof InterruptedException) Thread.currentThread().interrupt();
    return Errors.exception(ErrorReason.QUORUM_UNAVAILABLE);
  }
}
