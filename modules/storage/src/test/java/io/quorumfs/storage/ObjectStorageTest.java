package io.quorumfs.storage;

import static io.quorumfs.storage.StorageException.Code.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

class ObjectStorageTest {
  @TempDir Path root;
  private static final byte[] KEY = {0, 1, -1};

  private Path database() {
    return root.resolve("db");
  }

  private ObjectStorage open() throws IOException {
    return new ObjectStorage(database(), "node1/epoch1");
  }

  static ObjectStorage.Version put(ObjectStorage store, byte[] payload) throws IOException {
    try (var upload = store.begin("test", KEY, payload.length, ObjectStorage.sha256(payload))) {
      for (int offset = 0; offset < payload.length; offset += ObjectStorage.CHUNK_BYTES) {
        byte[] chunk =
            Arrays.copyOfRange(
                payload, offset, Math.min(payload.length, offset + ObjectStorage.CHUNK_BYTES));
        upload.append(offset, chunk, ObjectStorage.sha256(chunk));
      }
      return upload.commit();
    }
  }

  private static void code(
      StorageException.Code expected, org.junit.jupiter.api.function.Executable operation) {
    assertEquals(expected, assertThrows(StorageException.class, operation).code());
  }

  @Test
  void acknowledgedMultiChunkAndEmptyVersionsSurviveRestartAndCountersNeverRepeat()
      throws Exception {
    byte[] payload = new byte[ObjectStorage.CHUNK_BYTES * 2 + 1];
    new Random(20260930).nextBytes(payload);
    ObjectStorage.Version first, empty;
    try (var store = open()) {
      first = put(store, payload);
      empty = put(store, new byte[0]);
      try (var cancelled = store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0]))) {
        assertNotNull(cancelled);
      }
      assertEquals(2, store.verifyAll());
    }
    try (var store = open()) {
      var out = new ByteArrayOutputStream();
      store.read("test", KEY, first.id(), out);
      assertArrayEquals(payload, out.toByteArray());
      store.read("test", KEY, empty.id(), OutputStream.nullOutputStream());
      assertEquals(4, put(store, new byte[0]).sequence());
      code(NOT_FOUND, () -> store.head("other", KEY, first.id()));
      assertEquals(0, store.stats().recovered());
    }
  }

  @Test
  void stagedAndCancelledUploadsAreInvisibleAndCollected() throws Exception {
    UUID interrupted;
    try (var store = open()) {
      var pending = store.begin("test", KEY, 1, ObjectStorage.sha256(new byte[] {7}));
      interrupted = pending.id();
      pending.append(0, new byte[] {7}, ObjectStorage.sha256(new byte[] {7}));
      code(NOT_FOUND, () -> store.head("test", KEY, pending.id()));
      // Closing the store simulates losing an upload session; startup reclaims its persisted stage.
    }
    try (var store = open()) {
      assertEquals(1, store.stats().recovered());
      code(NOT_FOUND, () -> store.head("test", KEY, interrupted));
      try (var upload = store.begin("test", KEY, 1, ObjectStorage.sha256(new byte[] {9}))) {
        upload.append(0, new byte[] {9}, ObjectStorage.sha256(new byte[] {9}));
        upload.abort();
        code(CANCELLED, upload::commit);
      }
      assertEquals(0, store.stats().activeUploads());
    }
    raw(
        (db, handles) -> {
          assertEquals(0, count(db, handles.get("chunks")));
          assertEquals(0, count(db, handles.get("staging")));
        });
  }

  @Test
  void rejectsWrongOffsetsTruncationDuplicatesAndBothChecksums() throws Exception {
    try (var store = open()) {
      for (int scenario = 0; scenario < 5; scenario++) {
        var upload = store.begin("test", KEY, 1, ObjectStorage.sha256(new byte[] {1}));
        int selected = scenario;
        code(
            INVALID,
            () -> {
              switch (selected) {
                case 0 -> upload.append(1, new byte[] {1}, ObjectStorage.sha256(new byte[] {1}));
                case 1 -> upload.commit();
                case 2 -> upload.append(0, new byte[] {1}, new byte[32]);
                case 3 -> {
                  upload.append(0, new byte[] {2}, ObjectStorage.sha256(new byte[] {2}));
                  upload.commit();
                }
                case 4 -> {
                  upload.append(0, new byte[] {1}, ObjectStorage.sha256(new byte[] {1}));
                  upload.append(0, new byte[] {1}, ObjectStorage.sha256(new byte[] {1}));
                }
                default -> throw new AssertionError();
              }
            });
        assertEquals(0, store.stats().activeUploads());
        code(NOT_FOUND, () -> store.head("test", KEY, upload.id()));
      }
    }
  }

  @Test
  void validatesLimitsAndBoundsConcurrentUploadsAndExpiresAbandonedSessions() throws Exception {
    AtomicLong clock = new AtomicLong();
    try (var store =
        new ObjectStorage(
            database(),
            "node1/epoch1",
            new ObjectStorage.Limits(4, 1, Duration.ofSeconds(1)),
            point -> {},
            clock::get)) {
      code(INVALID, () -> store.begin("test", KEY, 5, new byte[32]));
      code(INVALID, () -> store.begin("test", new byte[1025], 1, new byte[32]));
      code(INVALID, () -> store.begin("../bad", KEY, 1, new byte[32]));
      var first = store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0]));
      code(CAPACITY, () -> store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0])));
      clock.set(Duration.ofSeconds(2).toNanos());
      try (var next = store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0]))) {
        assertNotNull(next);
      }
      code(CANCELLED, first::commit);
      assertEquals(0, store.stats().activeUploads());
    }
  }

  @Test
  void threadCancellationCleansStagingAndClosedStoreRejectsUse() throws Exception {
    var store = open();
    try {
      var upload = store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0]));
      Thread.currentThread().interrupt();
      try {
        code(CANCELLED, upload::commit);
      } finally {
        Thread.interrupted();
      }
      assertEquals(0, store.stats().activeUploads());
    } finally {
      store.close();
    }
    code(CLOSED, () -> store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0])));
  }

  @Test
  void durableWriteFailuresNeverReturnSuccessAndRequireReopen() throws Exception {
    for (var point :
        List.of(
            ObjectStorage.Point.COUNTER_WRITE,
            ObjectStorage.Point.STAGE_WRITE,
            ObjectStorage.Point.CHUNK_WRITE,
            ObjectStorage.Point.WAL_SYNC,
            ObjectStorage.Point.MANIFEST_WRITE,
            ObjectStorage.Point.AFTER_MANIFEST)) {
      Path path = root.resolve(point.name());
      UUID[] id = new UUID[1];
      try (var store =
          new ObjectStorage(
              path,
              "identity",
              ObjectStorage.Limits.defaults(),
              p -> {
                if (p == point) throw new IOException("injected disk-full/sync-error");
              },
              System::nanoTime)) {
        code(
            IO_FAILURE,
            () -> {
              var upload = store.begin("test", KEY, 1, ObjectStorage.sha256(new byte[] {1}));
              id[0] = upload.id();
              upload.append(0, new byte[] {1}, ObjectStorage.sha256(new byte[] {1}));
              upload.commit();
            });
        assertTrue(store.stats().failed());
        assertEquals(1, store.stats().ioFailures());
        code(IO_FAILURE, () -> store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0])));
      }
      try (var store = new ObjectStorage(path, "identity")) {
        assertEquals(point == ObjectStorage.Point.AFTER_MANIFEST ? 1 : 0, store.verifyAll());
        if (point == ObjectStorage.Point.AFTER_MANIFEST)
          store.read("test", KEY, id[0], OutputStream.nullOutputStream());
      }
    }
  }

  @Test
  void checkpointRestorePreservesObjectsIdentityAndCounterAndRefusesOverwrite() throws Exception {
    byte[] payload = {1, 2, 3};
    ObjectStorage.Version version;
    Path checkpoint = root.resolve("checkpoint"), restored = root.resolve("restored");
    try (var store = open()) {
      version = put(store, payload);
      store.checkpoint(checkpoint);
      try (var active = store.begin("test", KEY, 0, ObjectStorage.sha256(new byte[0]))) {
        assertNotNull(active);
        code(CAPACITY, () -> store.checkpoint(root.resolve("busy")));
      }
    }
    ObjectStorage.restore(checkpoint, restored, "node1/epoch1");
    try (var store = new ObjectStorage(restored, "node1/epoch1")) {
      var output = new ByteArrayOutputStream();
      store.read("test", KEY, version.id(), output);
      assertArrayEquals(payload, output.toByteArray());
      assertEquals(2, put(store, new byte[0]).sequence());
    }
    code(INVALID, () -> ObjectStorage.restore(checkpoint, restored, "node1/epoch1"));
    assertThrows(
        IOException.class,
        () -> ObjectStorage.restore(checkpoint, root.resolve("wrong"), "wrong-identity"));
    assertFalse(Files.exists(root.resolve("wrong")));
  }

  @Test
  void q0IdentityDatabaseUpgradesWithoutChangingIdentity() throws Exception {
    try (var original = new IdentityStore(database(), "node1/epoch1")) {
      assertNotNull(original);
    }
    try (var store = open()) {
      assertEquals(1, put(store, new byte[0]).sequence());
    }
    assertThrows(IOException.class, () -> new ObjectStorage(database(), "different"));
    try (var store = open()) {
      assertEquals(1, store.verifyAll());
    }
  }

  @Test
  void missingOrCorruptChunksFailRecoveryWithoutReturningUncheckedData() throws Exception {
    ObjectStorage.Version version;
    try (var store = open()) {
      version = put(store, new byte[] {1});
    }
    raw(
        (db, handles) ->
            db.put(handles.get("chunks"), StorageKeys.chunk(version.id(), 0), new byte[33]));
    code(CORRUPT, this::open);
    raw((db, handles) -> db.delete(handles.get("chunks"), StorageKeys.chunk(version.id(), 0)));
    code(CORRUPT, this::open);
  }

  @Test
  void corruptionWithRecomputedChunkHashStillFailsWholeObjectVerification() throws Exception {
    ObjectStorage.Version version;
    try (var store = open()) {
      version = put(store, new byte[] {1});
    }
    byte[] changed =
        ByteBuffer.allocate(33).put(ObjectStorage.sha256(new byte[] {2})).put((byte) 2).array();
    raw(
        (db, handles) ->
            db.put(handles.get("chunks"), StorageKeys.chunk(version.id(), 0), changed));
    code(CORRUPT, this::open);
  }

  @Test
  void namespacesAndBinaryKeysHaveUnambiguousEncoding() {
    UUID version = new UUID(1, 2);
    assertFalse(
        Arrays.equals(
            StorageKeys.manifest("ab", new byte[] {'c'}, version),
            StorageKeys.manifest("a", new byte[] {'b', 'c'}, version)));
    assertEquals(version, StorageKeys.id(StorageKeys.id(version)));
  }

  @Test
  void outputFailureDoesNotPoisonStoredData() throws Exception {
    try (var store = open()) {
      var version = put(store, new byte[] {1});
      assertThrows(
          IOException.class,
          () ->
              store.read(
                  "test",
                  KEY,
                  version.id(),
                  new OutputStream() {
                    @Override
                    public void write(int value) throws IOException {
                      throw new IOException("consumer disconnected");
                    }
                  }));
      assertFalse(store.stats().failed());
      store.read("test", KEY, version.id(), OutputStream.nullOutputStream());
    }
  }

  @Test
  void recoveryDoesNotCollectCommittedDataWhenStagingMetadataIsInconsistent() throws Exception {
    ObjectStorage.Version version;
    try (var store = open()) {
      version = put(store, new byte[] {1});
    }
    raw(
        (db, handles) -> {
          db.put(
              handles.get("staging"),
              StorageKeys.id(version.id()),
              StorageKeys.manifest("test", KEY, version.id()));
          db.delete(handles.get("requests"), StorageKeys.id(version.id()));
        });
    code(CORRUPT, this::open);
    raw((db, handles) -> assertEquals(1, count(db, handles.get("chunks"))));
  }

  @Test
  void missingCounterOrVersionIndexFailsClosed() throws Exception {
    ObjectStorage.Version version;
    try (var store = open()) {
      version = put(store, new byte[] {1});
    }
    raw((db, handles) -> db.delete(handles.get("counters"), new byte[] {1}));
    code(CORRUPT, this::open);
    raw(
        (db, handles) -> {
          db.put(
              handles.get("counters"), new byte[] {1}, ByteBuffer.allocate(8).putLong(1).array());
          db.delete(handles.get("requests"), StorageKeys.id(version.id()));
        });
    code(CORRUPT, this::open);
  }

  @Test
  void concurrentUploadsRetainEveryImmutableVersion() throws Exception {
    try (var store = open();
        var pool = java.util.concurrent.Executors.newFixedThreadPool(8)) {
      var barrier = new java.util.concurrent.CyclicBarrier(8);
      List<java.util.concurrent.Future<ObjectStorage.Version>> results = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        final byte value = (byte) i;
        results.add(
            pool.submit(
                () -> {
                  byte[] payload = {value};
                  try (var upload = store.begin("test", KEY, 1, ObjectStorage.sha256(payload))) {
                    barrier.await(10, java.util.concurrent.TimeUnit.SECONDS);
                    upload.append(0, payload, ObjectStorage.sha256(payload));
                    return upload.commit();
                  }
                }));
      }
      Set<Long> sequences = new HashSet<>();
      for (int i = 0; i < results.size(); i++) {
        var version = results.get(i).get(15, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(sequences.add(version.sequence()));
        var output = new ByteArrayOutputStream();
        store.read("test", KEY, version.id(), output);
        assertArrayEquals(new byte[] {(byte) i}, output.toByteArray());
      }
      assertEquals(8, store.verifyAll());
      assertEquals(0, store.stats().activeUploads());
    }
  }

  @Test
  void corruptCheckpointIsNeverPublishedAsARestoredDatabase() throws Exception {
    Path checkpoint = root.resolve("corrupt-checkpoint"), target = root.resolve("restore-target");
    ObjectStorage.Version version;
    try (var store = open()) {
      version = put(store, new byte[] {1});
      store.checkpoint(checkpoint);
    }
    raw(
        checkpoint,
        (db, handles) -> db.delete(handles.get("chunks"), StorageKeys.chunk(version.id(), 0)));
    code(CORRUPT, () -> ObjectStorage.restore(checkpoint, target, "node1/epoch1"));
    assertFalse(Files.exists(target));
    try (var store = open()) {
      assertEquals(1, store.verifyAll());
    }
  }

  private static long count(RocksDB db, ColumnFamilyHandle handle) throws RocksDBException {
    long count = 0;
    try (var iterator = db.newIterator(handle)) {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) count++;
      iterator.status();
    }
    return count;
  }

  @FunctionalInterface
  interface RawAction {
    void run(RocksDB db, Map<String, ColumnFamilyHandle> handles) throws Exception;
  }

  private void raw(RawAction action) throws Exception {
    raw(database(), action);
  }

  private void raw(Path path, RawAction action) throws Exception {
    List<ColumnFamilyOptions> options = new ArrayList<>();
    List<ColumnFamilyHandle> handles = new ArrayList<>();
    try (DBOptions dbOptions = new DBOptions()) {
      List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
      for (String family : ObjectStorage.FAMILIES) {
        var option = new ColumnFamilyOptions();
        options.add(option);
        descriptors.add(
            new ColumnFamilyDescriptor(
                family.getBytes(java.nio.charset.StandardCharsets.UTF_8), option));
      }
      try (RocksDB db = RocksDB.open(dbOptions, path.toString(), descriptors, handles)) {
        Map<String, ColumnFamilyHandle> map = new HashMap<>();
        for (int i = 0; i < handles.size(); i++)
          map.put(ObjectStorage.FAMILIES.get(i), handles.get(i));
        try {
          action.run(db, map);
        } finally {
          handles.forEach(ColumnFamilyHandle::close);
        }
      }
    } finally {
      options.forEach(ColumnFamilyOptions::close);
    }
  }
}
