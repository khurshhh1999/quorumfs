package io.quorumfs.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.quorumfs.versioning.VectorClock;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

class RecoveryStorageTest {
  @TempDir Path root;
  private static final byte[] KEY = {1}, EMPTY = ObjectStorage.sha256(new byte[0]);

  private ObjectStorage store(Path path) throws Exception {
    var store = new ObjectStorage(path, "recovery");
    store.configureRing(CausalStorageTest.ring(), "node1");
    return store;
  }

  private UUID replica(ObjectStorage store, VectorClock vector, boolean tombstone)
      throws Exception {
    UUID id = UUID.randomUUID();
    try (var upload =
        store.beginReplica("ns", KEY, 0, EMPTY, vector, CausalStorageTest.ring(), id, tombstone)) {
      upload.commit();
    }
    return id;
  }

  private void enqueue(ObjectStorage store, UUID id, byte[] data, List<String> targets)
      throws Exception {
    store.enqueueHint(
        id,
        new byte[] {42},
        targets,
        data.length,
        ObjectStorage.sha256(data),
        new ByteArrayInputStream(data));
  }

  @Test
  void tombstoneDominanceConflictStaleReplayAndCheckpointPreserveDeletion() throws Exception {
    Path db = root.resolve("db"), backup = root.resolve("backup");
    UUID deleted;
    var prior = VectorClock.of(Map.of("node2", 1L));
    var deletion = VectorClock.of(Map.of("node2", 2L));
    try (var store = store(db)) {
      replica(store, prior, false);
      deleted = replica(store, deletion, true);
      replica(store, prior, false); // Late pre-delete replay must not resurrect a live head.
      assertEquals(List.of(deleted), store.siblings("ns", KEY).stream().map(e -> e.id()).toList());
      assertTrue(store.head("ns", KEY, deleted).tombstone());
      replica(store, VectorClock.of(Map.of("node3", 1L)), false);
      assertEquals(2, store.siblings("ns", KEY).size());
      assertEquals(1, store.siblings("ns", KEY).stream().filter(e -> e.tombstone()).count());
      store.checkpoint(backup);
    }
    ObjectStorage.restore(backup, root.resolve("restored"), "recovery");
    for (Path path : List.of(db, root.resolve("restored"))) {
      try (var store = store(path)) {
        assertEquals(4, store.verifyAll());
        assertEquals(2, store.siblings("ns", KEY).size());
        assertTrue(store.head("ns", KEY, deleted).tombstone());
      }
    }
  }

  @Test
  void emptyLiveAndTombstoneCannotShareVectorEvenAfterSuperseded() throws Exception {
    try (var store = store(root.resolve("db"))) {
      var first = VectorClock.of(Map.of("node2", 1L));
      replica(store, first, false);
      replica(store, VectorClock.of(Map.of("node2", 2L)), true);
      assertEquals(
          StorageException.Code.CORRUPT,
          assertThrows(StorageException.class, () -> replica(store, first, true)).code());
      assertThrows(
          StorageException.class,
          () ->
              store.beginReplica(
                  "ns",
                  KEY,
                  1,
                  ObjectStorage.sha256(new byte[] {1}),
                  first,
                  CausalStorageTest.ring(),
                  UUID.randomUUID(),
                  true));
    }
  }

  @Test
  void hintPayloadAndPartialAcknowledgmentsSurviveRestartAndCheckpoint() throws Exception {
    Path db = root.resolve("db"), backup = root.resolve("backup");
    UUID id = UUID.randomUUID();
    byte[] data = new byte[ObjectStorage.CHUNK_BYTES + 17];
    new Random(20261008).nextBytes(data);
    try (var store = store(db)) {
      enqueue(store, id, data, List.of("node2", "node3"));
      store.acknowledgeHint(id, "node2");
      assertEquals(List.of("node3"), store.hints().getFirst().destinations());
      assertEquals(0, store.verifyAll()); // Hints never publish non-owner object versions.
      store.checkpoint(backup);
    }
    ObjectStorage.restore(backup, root.resolve("restored"), "recovery");
    for (Path path : List.of(db, root.resolve("restored"))) {
      try (var store = store(path)) {
        var out = new ByteArrayOutputStream();
        store.readHint(id, out);
        assertArrayEquals(data, out.toByteArray());
        assertEquals(data.length, store.hintStats().bytes());
        store.acknowledgeHint(id, "node5");
        assertEquals(1, store.hints().size());
        store.acknowledgeHint(id, "node3");
        store.acknowledgeHint(id, "node3");
        assertTrue(store.hints().isEmpty());
        assertEquals(0, store.hintStats().bytes());
        store.verifyAll();
      }
    }
  }

  @Test
  void hintAdmissionIsBoundedAndDuplicateMergesTargetsWithoutCopyingPayload() throws Exception {
    try (var store = store(root.resolve("db"))) {
      UUID id = UUID.randomUUID();
      enqueue(store, id, new byte[0], List.of("node2"));
      enqueue(store, id, new byte[0], List.of("node3"));
      assertEquals(2, store.hintStats().pendingDeliveries());
      for (int i = 1; i < ObjectStorage.MAX_HINTS; i++)
        enqueue(store, UUID.randomUUID(), new byte[0], List.of("node2"));
      assertEquals(
          StorageException.Code.CAPACITY,
          assertThrows(
                  StorageException.class,
                  () -> enqueue(store, UUID.randomUUID(), new byte[0], List.of("node2")))
              .code());
      assertEquals(ObjectStorage.MAX_HINTS, store.hints().size());
      assertEquals(
          StorageException.Code.CORRUPT,
          assertThrows(
                  StorageException.class,
                  () -> enqueue(store, id, new byte[] {1}, List.of("node2")))
              .code());
      store.acknowledgeHint(id, "node2");
      store.acknowledgeHint(id, "node3");
      enqueue(store, UUID.randomUUID(), new byte[0], List.of("node2"));
    }
  }

  @Test
  void failedCaptureCannotPublishPromiseOrLeakBudget() throws Exception {
    try (var store = store(root.resolve("db"))) {
      assertThrows(
          StorageException.class,
          () ->
              store.enqueueHint(
                  UUID.randomUUID(),
                  new byte[] {1},
                  List.of("node2"),
                  2,
                  EMPTY,
                  new ByteArrayInputStream(new byte[] {1})));
      assertThrows(
          StorageException.class,
          () ->
              store.enqueueHint(
                  UUID.randomUUID(),
                  new byte[] {1},
                  List.of("node2"),
                  1,
                  EMPTY,
                  new ByteArrayInputStream(new byte[] {1})));
      assertTrue(store.hints().isEmpty());
      assertEquals(0, store.hintStats().bytes());
      store.verifyAll();
    }
  }

  @Test
  void priorFormatUpgradesWithoutLosingCausalHistory() throws Exception {
    Path path = root.resolve("db");
    UUID id;
    try (var store = store(path)) {
      id = replica(store, VectorClock.of(Map.of("node2", 1L)), false);
    }
    // Real prior-format fixture: live manifests and causal indexes are unchanged from format 1.
    withDb(
        path,
        (db, handles) ->
            db.put(
                "storage-format".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                new byte[] {1}));
    try (var store = store(path)) {
      assertFalse(store.head("ns", KEY, id).tombstone());
      assertEquals(1, store.verifyAll());
    }
    withDb(
        path,
        (db, handles) ->
            assertArrayEquals(
                new byte[] {2},
                db.get("storage-format".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
  }

  @Test
  void corruptPinnedBytesFailStartupAndAreNeverSilentlyDiscarded() throws Exception {
    Path path = root.resolve("db");
    UUID id = UUID.randomUUID();
    try (var store = store(path)) {
      enqueue(store, id, new byte[] {1, 2, 3}, List.of("node2"));
    }
    withDb(
        path,
        (db, handles) -> {
          var cf = handles.get(ObjectStorage.FAMILIES.indexOf("hints"));
          byte[] chunk = db.get(cf, StorageKeys.chunk(id, 0));
          chunk[32] ^= 1;
          db.put(cf, StorageKeys.chunk(id, 0), chunk);
        });
    assertEquals(
        StorageException.Code.CORRUPT,
        assertThrows(StorageException.class, () -> store(path)).code());
    withDb(
        path,
        (db, handles) ->
            assertNotNull(
                db.get(handles.get(ObjectStorage.FAMILIES.indexOf("hints")), StorageKeys.id(id))));
  }

  @FunctionalInterface
  private interface DatabaseEdit {
    void run(RocksDB db, List<ColumnFamilyHandle> handles) throws Exception;
  }

  private void withDb(Path path, DatabaseEdit edit) throws Exception {
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    List<ColumnFamilyOptions> options = new ArrayList<>();
    try (var dbOptions = new DBOptions()) {
      var descriptors =
          ObjectStorage.FAMILIES.stream()
              .map(
                  name -> {
                    var option = new ColumnFamilyOptions();
                    options.add(option);
                    return new ColumnFamilyDescriptor(
                        name.getBytes(java.nio.charset.StandardCharsets.UTF_8), option);
                  })
              .toList();
      try (var db = RocksDB.open(dbOptions, path.toString(), descriptors, handles)) {
        try {
          edit.run(db, handles);
        } finally {
          handles.forEach(ColumnFamilyHandle::close);
        }
      } finally {
        options.forEach(ColumnFamilyOptions::close);
      }
    }
  }
}
