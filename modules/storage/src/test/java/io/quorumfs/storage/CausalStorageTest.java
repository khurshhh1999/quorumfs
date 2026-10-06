package io.quorumfs.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.quorumfs.ring.HashRing;
import io.quorumfs.versioning.VectorClock;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CausalStorageTest {
  @TempDir Path root;

  static HashRing ring() {
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

  private ObjectStorage store(Path path) throws Exception {
    var store = new ObjectStorage(path, "causal-test");
    store.configureRing(ring(), "node1");
    return store;
  }

  private static final byte[] KEY = {1}, EMPTY = ObjectStorage.sha256(new byte[0]);

  private static UUID replica(ObjectStorage store, VectorClock vector) throws Exception {
    try (var upload = store.beginReplica("ns", KEY, 0, EMPTY, vector, ring())) {
      return upload.commit().id();
    }
  }

  @Test
  void concurrentSiblingsResolveOnlyObservedVersionsAndSurviveRestartAndRestore() throws Exception {
    Path db = root.resolve("db"), backup = root.resolve("backup");
    UUID first, second, racing, resolved;
    try (var store = store(db)) {
      first = replica(store, VectorClock.of(Map.of("node2", 1L)));
      second = replica(store, VectorClock.of(Map.of("node3", 1L)));
      assertEquals(2, store.siblings("ns", KEY).size());
      try (var resolution = store.resolve("ns", KEY, 0, EMPTY, List.of(first, second))) {
        racing = replica(store, VectorClock.of(Map.of("node4", 1L)));
        resolved = resolution.commit().id();
      }
      assertEquals(
          Set.of(resolved, racing),
          new HashSet<>(store.siblings("ns", KEY).stream().map(e -> e.id()).toList()));
      store.checkpoint(backup);
    }
    try (var store = store(db)) {
      assertEquals(4, store.verifyAll());
      assertEquals(2, store.siblings("ns", KEY).size());
      try (var resolution = store.resolve("ns", KEY, 0, EMPTY, List.of(resolved, racing))) {
        resolution.commit();
      }
      assertEquals(1, store.siblings("ns", KEY).size());
    }
    ObjectStorage.restore(backup, root.resolve("restore"), "causal-test");
    try (var restored = store(root.resolve("restore"))) {
      assertEquals(2, restored.siblings("ns", KEY).size());
    }
  }

  @Test
  void counterExceedsSuppliedContextAndCancelledAllocationAcrossRestart() throws Exception {
    Path db = root.resolve("db");
    try (var store = store(db);
        var upload =
            store.beginCausal("ns", KEY, 0, EMPTY, VectorClock.of(Map.of("node1", 100L)))) {
      assertNotNull(upload.id());
    }
    try (var store = store(db);
        var upload = store.beginCausal("ns", KEY, 0, EMPTY, VectorClock.empty())) {
      UUID id = upload.commit().id();
      assertEquals(102, store.vector(id).get("node1"));
      assertThrows(
          StorageException.class,
          () ->
              store.beginCausal(
                  "ns", KEY, 0, EMPTY, VectorClock.of(Map.of("node1", Long.MAX_VALUE))));
    }
  }

  @Test
  void rejectsSameVectorDifferentContentEvenAfterItWasSuperseded() throws Exception {
    try (var store = store(root.resolve("db"))) {
      VectorClock first = VectorClock.of(Map.of("node2", 1L));
      replica(store, first);
      replica(store, VectorClock.of(Map.of("node2", 2L)));
      try (var bad =
          store.beginReplica("ns", KEY, 1, ObjectStorage.sha256(new byte[] {1}), first, ring())) {
        bad.append(0, new byte[] {1}, ObjectStorage.sha256(new byte[] {1}));
        assertEquals(
            StorageException.Code.CORRUPT,
            assertThrows(StorageException.class, bad::commit).code());
      }
      assertEquals(2, store.verifyAll());
      assertEquals(1, store.siblings("ns", KEY).size());
    }
  }

  @Test
  void siblingCapacityRejectsWithoutDroppingVersionsAndResolutionFreesCapacity() throws Exception {
    try (var store = store(root.resolve("db"))) {
      for (int i = 1; i <= 32; i++)
        replica(store, VectorClock.of(Map.of("node2", (long) i, "node3", 34L - i)));
      try (var upload =
          store.beginReplica(
              "ns", KEY, 0, EMPTY, VectorClock.of(Map.of("node2", 33L, "node3", 1L)), ring())) {
        assertEquals(
            StorageException.Code.CAPACITY,
            assertThrows(StorageException.class, upload::commit).code());
      }
      assertEquals(32, store.siblings("ns", KEY).size());
      try (var upload =
          store.resolve(
              "ns", KEY, 0, EMPTY, store.siblings("ns", KEY).stream().map(e -> e.id()).toList())) {
        upload.commit();
      }
      assertEquals(1, store.siblings("ns", KEY).size());
      assertEquals(33, store.verifyAll());
    }
  }

  @Test
  void persistedRingAndPeerMismatchFailClosed() throws Exception {
    Path db = root.resolve("db");
    try (var store = store(db)) {
      HashRing wrong =
          new HashRing(
              "test",
              2,
              java.util.stream.IntStream.rangeClosed(1, 5)
                  .mapToObj(i -> new HashRing.Member("node" + i, "localhost:" + (9000 + i)))
                  .toList(),
              3,
              2,
              2);
      assertThrows(
          IllegalArgumentException.class,
          () ->
              store.beginReplica("ns", KEY, 0, EMPTY, VectorClock.of(Map.of("node2", 1L)), wrong));
      assertThrows(IllegalArgumentException.class, () -> store.configureRing(wrong, "node1"));
      assertThrows(StorageException.class, () -> store.configureRing(ring(), "node2"));
      assertThrows(
          IllegalArgumentException.class,
          () -> store.beginCausal("ns", KEY, 0, EMPTY, VectorClock.of(Map.of("stranger", 1L))));
    }
    try (var store = store(db)) {
      assertEquals(0, store.verifyAll());
    }
  }

  @Test
  void legacyObjectsStayReadableAndCannotBeSilentlyAssignedCausality() throws Exception {
    try (var store = store(root.resolve("db"))) {
      try (var upload = store.begin("ns", KEY, 0, EMPTY)) {
        upload.commit();
      }
      assertThrows(
          StorageException.class,
          () -> store.beginCausal("ns", KEY, 0, EMPTY, VectorClock.empty()));
      assertEquals(1, store.verifyAll());
    }
  }

  @Test
  void overlappingLegacyAndCausalUploadsCannotMixFormats() throws Exception {
    try (var store = store(root.resolve("db"));
        var legacy = store.begin("ns", KEY, 0, EMPTY);
        var causal = store.beginCausal("ns", KEY, 0, EMPTY, VectorClock.empty())) {
      causal.commit();
      assertEquals(
          StorageException.Code.INVALID,
          assertThrows(StorageException.class, legacy::commit).code());
      assertEquals(1, store.verifyAll());
    }
  }

  @Test
  void missingOrCorruptCausalMetadataFailsStartupAndRestore() throws Exception {
    for (String damaged : List.of("vectors", "heads", "ring")) {
      Path db = root.resolve(damaged), backup = root.resolve(damaged + "-backup");
      UUID id;
      try (var store = store(db)) {
        id = replica(store, VectorClock.of(Map.of("node2", 1L)));
        store.checkpoint(backup);
      }
      ObjectStorageTest.raw(
          db,
          (nativeDb, families) -> {
            byte[] key =
                damaged.equals("vectors")
                    ? StorageKeys.id(id)
                    : damaged.equals("heads")
                        ? StorageKeys.manifest("ns", KEY, id)
                        : new byte[] {1};
            nativeDb.delete(families.get(damaged), key);
          });
      assertThrows(java.io.IOException.class, () -> new ObjectStorage(db, "causal-test"));
      ObjectStorageTest.raw(
          backup,
          (nativeDb, families) -> {
            nativeDb.put(
                families.get("vectors"),
                StorageKeys.id(id),
                VectorClock.of(Map.of("node5", 9L)).serialize());
          });
      Path destination = root.resolve(damaged + "-restore");
      assertThrows(
          java.io.IOException.class,
          () -> ObjectStorage.restore(backup, destination, "causal-test"));
      assertFalse(java.nio.file.Files.exists(destination));
    }
  }

  @Test
  void fiveLocalReplicasConvergeAcrossOppositeArrivalOrders() throws Exception {
    List<VectorClock> versions =
        List.of(
            VectorClock.of(Map.of("node2", 1L)),
            VectorClock.of(Map.of("node3", 1L)),
            VectorClock.of(Map.of("node2", 2L)),
            VectorClock.of(Map.of("node4", 1L)),
            VectorClock.of(Map.of("node2", 2L, "node3", 1L)));
    Set<VectorClock> expected = Set.of(versions.get(3), versions.get(4));
    Random random = new Random(20261006);
    for (int i = 0; i < 5; i++) {
      List<VectorClock> order = new ArrayList<>(versions);
      Collections.shuffle(order, random);
      Path db = root.resolve("replica" + i);
      try (var store = store(db)) {
        for (var vector : order) replica(store, vector);
      }
      try (var store = store(db)) {
        assertEquals(
            expected,
            new HashSet<>(store.siblings("ns", KEY).stream().map(e -> e.clock()).toList()));
        assertEquals(5, store.verifyAll());
      }
    }
  }
}
