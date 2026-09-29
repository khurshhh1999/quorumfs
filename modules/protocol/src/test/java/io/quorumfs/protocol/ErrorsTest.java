package io.quorumfs.protocol;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Status;
import io.quorumfs.protocol.v1.*;
import org.junit.jupiter.api.Test;

class ErrorsTest {
  @Test
  void explicitErrorsRoundTripThroughTrailers() throws Exception {
    for (var reason : ErrorReason.values()) {
      if (reason == ErrorReason.UNRECOGNIZED || reason == ErrorReason.ERROR_REASON_UNSPECIFIED)
        continue;
      var error = Errors.exception(reason);
      var detail = ErrorDetail.parseFrom(error.getTrailers().get(Errors.DETAIL));
      assertEquals(reason, detail.getReason());
      assertEquals(reason == ErrorReason.OUTCOME_UNKNOWN, detail.getOutcomeUnknown());
    }
    assertEquals(
        Status.Code.DEADLINE_EXCEEDED,
        Errors.exception(ErrorReason.OUTCOME_UNKNOWN).getStatus().getCode());
    assertEquals(
        Status.Code.UNAVAILABLE,
        Errors.exception(ErrorReason.QUORUM_UNAVAILABLE).getStatus().getCode());
  }

  @Test
  void unspecifiedErrorsAreRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> Errors.exception(ErrorReason.ERROR_REASON_UNSPECIFIED));
  }
}
