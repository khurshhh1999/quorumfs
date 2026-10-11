package io.quorumfs.storage;

import static org.junit.jupiter.api.Assertions.*;

import io.quorumfs.versioning.VectorClock;
import java.io.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

class AntiEntropyStorageTest {
  @TempDir Path root;
  private final byte[] key = {1};

  private ObjectStorage store(Path path) throws Exception {
    var store = new ObjectStorage(path, "anti-entropy");
    store.configureRing(CausalStorageTest.ring(), "node1");
    return store;
  }

  private UUID put(ObjectStorage store, byte[] payload) throws Exception {
    try (var upload =
        store.beginCausal(
            "ns", key, payload.length, ObjectStorage.sha256(payload), VectorClock.empty())) {
      for (int i = 0; i < payload.length; i += ObjectStorage.CHUNK_BYTES) {
        byte[] part =
            Arrays.copyOfRange(payload, i, Math.min(payload.length, i + ObjectStorage.CHUNK_BYTES));
        upload.append(i, part, ObjectStorage.sha256(part));
      }
      return upload.commit().id();
    }
  }

  @Test
  void damagedPayloadIsDetectedRepairedAtomicallyAndSurvivesRestore() throws Exception {
    Path path = root.resolve("db"), backup = root.resolve("backup");
    byte[] payload = new byte[ObjectStorage.CHUNK_BYTES + 19];
    new Random(20261011).nextBytes(payload);
    UUID id;
    long sequence;
    try (var store = store(path)) {
      id = put(store, payload);
      sequence = store.head("ns", key, id).sequence();
      String peer =
          CausalStorageTest.ring().owners("ns", key).stream()
              .filter(n -> !n.equals("node1"))
              .findFirst()
              .orElseThrow();
      // Choose a shared key for inventory; corruption itself is checked independently of ownership.
      var dbField = ObjectStorage.class.getDeclaredField("db");
      dbField.setAccessible(true);
      var cf = ObjectStorage.class.getDeclaredMethod("cf", String.class);
      cf.setAccessible(true);
      RocksDB db = (RocksDB) dbField.get(store);
      db.put(
          (ColumnFamilyHandle) cf.invoke(store, "chunks"),
          StorageKeys.chunk(id, 0),
          new byte[] {0});
      assertEquals(
          StorageException.Code.CORRUPT,
          assertThrows(
                  StorageException.class,
                  () -> store.read("ns", key, id, OutputStream.nullOutputStream()))
              .code());
      if (CausalStorageTest.ring().owners("ns", key).contains("node1"))
        assertFalse(store.inventory(peer).getFirst().healthy());
      assertThrows(
          StorageException.class,
          () -> store.repair("ns", key, id, new ByteArrayInputStream(new byte[] {1})));
      store.repair("ns", key, id, new ByteArrayInputStream(payload));
      var version = store.head("ns", key, id);
      assertNotEquals(id, version.payloadId());
      assertEquals(sequence, version.sequence());
      assertThrows(
          StorageException.class,
          () ->
              store.beginReplica(
                  "ns",
                  new byte[] {2},
                  0,
                  ObjectStorage.sha256(new byte[0]),
                  VectorClock.of(Map.of("node2", 1L)),
                  CausalStorageTest.ring(),
                  version.payloadId()));
      var out = new ByteArrayOutputStream();
      store.read("ns", key, id, out);
      assertArrayEquals(payload, out.toByteArray());
      assertEquals(1, store.verifyAll());
      store.checkpoint(backup);
    }
    ObjectStorage.restore(backup, root.resolve("restored"), "anti-entropy");
    for (Path directory : List.of(path, root.resolve("restored")))
      try (var store = store(directory)) {
        assertEquals(1, store.verifyAll());
        var out = new ByteArrayOutputStream();
        store.read("ns", key, id, out);
        assertArrayEquals(payload, out.toByteArray());
      }
  }

  @Test
  void rejectedReplacementCannotChangeHealthyCommittedBytes() throws Exception {
    try (var store = store(root.resolve("db"))) {
      byte[] bytes = {1, 2, 3};
      UUID id = put(store, bytes);
      assertThrows(
          StorageException.class,
          () -> store.repair("ns", key, id, new ByteArrayInputStream(new byte[] {4, 5, 6})));
      assertEquals(id, store.head("ns", key, id).payloadId());
      var out = new ByteArrayOutputStream();
      store.read("ns", key, id, out);
      assertArrayEquals(bytes, out.toByteArray());
      assertEquals(1, store.verifyAll());
    }
  }
}
