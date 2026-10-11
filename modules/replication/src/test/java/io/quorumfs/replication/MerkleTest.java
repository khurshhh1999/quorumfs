package io.quorumfs.replication;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.storage.ObjectStorage;
import io.quorumfs.versioning.VectorClock;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MerkleTest {
  @TempDir Path root;

  private HashRing ring() {
    return new HashRing(
        "merkle",
        1,
        java.util.stream.IntStream.rangeClosed(1, 5)
            .mapToObj(i -> new HashRing.Member("node" + i, "localhost:" + (9000 + i)))
            .toList(),
        3,
        2,
        2);
  }

  private ObjectKey key(int i) {
    return ObjectKey.newBuilder()
        .setNamespace("demo")
        .setKey(ByteString.copyFromUtf8("key" + i))
        .build();
  }

  private RangeVersion entry(int i) {
    return RangeVersion.newBuilder()
        .setObject(key(i))
        .setVersion(
            VersionMetadata.newBuilder()
                .setVersionId(new UUID(0, i + 1).toString())
                .setVector(VersionVector.newBuilder().putCounters("node1", i + 1L))
                .setSha256(Wire.hash(new byte[0])))
        .build();
  }

  @Test
  void treeIsPermutationIndependentAndCoversVectorsTombstonesAndCorruption() {
    var entries = new ArrayList<RangeVersion>();
    for (int i = 0; i < 500; i++) entries.add(entry(i));
    var expected = Merkle.tree(entries, 0);
    for (int seed = 0; seed < 10; seed++) {
      Collections.shuffle(entries, new Random(20261011L + seed));
      assertEquals(expected, Merkle.tree(entries, 0));
    }
    var original = entries.getFirst();
    for (var changed :
        List.of(
            original.toBuilder().setCorrupt(true).build(),
            original.toBuilder()
                .setVersion(original.getVersion().toBuilder().setTombstone(true))
                .build(),
            original.toBuilder()
                .setVersion(
                    original.getVersion().toBuilder()
                        .setVector(VersionVector.newBuilder().putCounters("node2", 1)))
                .build())) {
      entries.set(0, changed);
      assertNotEquals(expected, Merkle.tree(entries, 0));
    }
    entries.set(0, original);
    assertEquals(expected, Merkle.tree(entries, 0));
    for (int depth = 0; depth <= 4; depth++) {
      var nodes = Merkle.tree(entries, depth);
      assertEquals(1 << depth, nodes.size());
      for (int i = 0; i < nodes.size(); i++)
        assertEquals(i, Merkle.index(nodes.get(i).getRange(), depth));
    }
    assertThrows(RuntimeException.class, () -> Merkle.index(TokenRange.getDefaultInstance(), 0));
  }

  @Test
  void snapshotPaginationIsStableAcrossMutationAndRejectsExpiredOrForeignCursors()
      throws Exception {
    HashRing ring = ring();
    AtomicLong clock = new AtomicLong();
    try (var store = new ObjectStorage(root.resolve("db"), "merkle")) {
      store.configureRing(ring, "node1");
      int seed = 0, count = 0;
      while (count < 130) {
        ObjectKey key = key(seed++);
        if (Merkle.leaf(key) != 0
            || !ring.owners("demo", key.getKey().toByteArray())
                .containsAll(List.of("node1", "node2"))) continue;
        try (var upload =
            store.beginCausal(
                "demo",
                key.getKey().toByteArray(),
                0,
                Wire.hash(new byte[0]).toByteArray(),
                VectorClock.empty())) {
          upload.commit();
        }
        count++;
      }
      var service = new MerkleService(store, ring, "node1", clock::get);
      var request =
          MerkleRequest.newBuilder()
              .setCluster(Wire.identity(ring))
              .setPeerNodeId("node2")
              .setRange(Merkle.range(0, 0))
              .build();
      var first = service.exchange(request);
      String snapshot = first.getSnapshotId();
      ObjectKey added;
      do {
        added = key(seed++);
      } while (Merkle.leaf(added) != 0
          || !ring.owners("demo", added.getKey().toByteArray())
              .containsAll(List.of("node1", "node2")));
      try (var upload =
          store.beginCausal(
              "demo",
              added.getKey().toByteArray(),
              0,
              Wire.hash(new byte[0]).toByteArray(),
              VectorClock.empty())) {
        upload.commit();
      }
      assertEquals(first, service.exchange(request.toBuilder().setSnapshotId(snapshot).build()));
      var page =
          RangeRequest.newBuilder()
              .setCluster(Wire.identity(ring))
              .setPeerNodeId("node2")
              .setSnapshotId(snapshot)
              .setRange(Merkle.range(4, 0))
              .setLimit(128)
              .build();
      var a = service.page(page);
      assertEquals(128, a.size());
      var b = service.page(page.toBuilder().setResumeToken(a.getLast().getResumeToken()).build());
      assertEquals(2, b.size());
      assertThrows(
          RuntimeException.class,
          () -> service.page(page.toBuilder().setPeerNodeId("node3").build()));
      assertThrows(
          RuntimeException.class,
          () ->
              service.page(
                  page.toBuilder()
                      .setRange(Merkle.range(4, 1))
                      .setResumeToken(a.getLast().getResumeToken())
                      .build()));
      assertThrows(
          RuntimeException.class, () -> service.page(page.toBuilder().setLimit(129).build()));
      assertThrows(
          RuntimeException.class,
          () ->
              service.exchange(
                  request.toBuilder()
                      .setCluster(Wire.identity(ring).toBuilder().setEpoch(2))
                      .build()));
      clock.set(java.util.concurrent.TimeUnit.SECONDS.toNanos(61));
      assertThrows(
          RuntimeException.class,
          () -> service.exchange(request.toBuilder().setSnapshotId(snapshot).build()));
      assertNotEquals(first.getNodesList(), service.exchange(request).getNodesList());
    }
  }
}
