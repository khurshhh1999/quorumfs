package io.quorumfs.protocol;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import java.util.*;
import org.junit.jupiter.api.Test;

class WireTest {
  private HashRing ring() {
    return new HashRing(
        "test",
        1,
        java.util.stream.IntStream.rangeClosed(1, 5)
            .mapToObj(i -> new HashRing.Member("node" + i, "localhost:" + (9000 + i)))
            .toList(),
        3,
        2,
        2);
  }

  @Test
  void identityIncludesFullConfigurationAndMalformedUnsignedCountersFailClosed() {
    HashRing ring = ring();
    Wire.compatible(Wire.identity(ring), ring);
    assertEquals(
        Status.Code.FAILED_PRECONDITION,
        Status.fromThrowable(
                assertThrows(
                    RuntimeException.class,
                    () ->
                        Wire.compatible(
                            Wire.identity(ring).toBuilder().clearRingConfiguration().build(),
                            ring)))
            .getCode());
    assertThrows(
        RuntimeException.class,
        () -> Wire.vector(VersionVector.newBuilder().putCounters("node1", -1L).build(), ring));
    assertThrows(
        RuntimeException.class,
        () -> Wire.vector(VersionVector.newBuilder().putCounters("stranger", 1L).build(), ring));
    assertThrows(
        RuntimeException.class, () -> Wire.content(-1L, ByteString.copyFrom(new byte[32])));
    assertThrows(RuntimeException.class, () -> Wire.uuid("1-1-1-1-1"));
  }

  @Test
  void sameIdCannotHaveDifferentMetadataAndSameVectorCannotHaveDifferentContent() {
    var a =
        VersionMetadata.newBuilder()
            .setVersionId(UUID.randomUUID().toString())
            .setVector(VersionVector.newBuilder().putCounters("node1", 1))
            .setSize(0)
            .setSha256(Wire.hash(new byte[0]))
            .build();
    assertEquals(List.of(a), Wire.merge(List.of(a, a), ring()));
    assertEquals(
        Status.Code.DATA_LOSS,
        Status.fromThrowable(
                assertThrows(
                    RuntimeException.class,
                    () -> Wire.merge(List.of(a, a.toBuilder().setSize(1).build()), ring())))
            .getCode());
    assertEquals(
        Status.Code.DATA_LOSS,
        Status.fromThrowable(
                assertThrows(
                    RuntimeException.class,
                    () ->
                        Wire.merge(
                            List.of(
                                a,
                                a.toBuilder()
                                    .setVersionId(UUID.randomUUID().toString())
                                    .setSha256(Wire.hash(new byte[] {1}))
                                    .build()),
                            ring())))
            .getCode());
  }
}
