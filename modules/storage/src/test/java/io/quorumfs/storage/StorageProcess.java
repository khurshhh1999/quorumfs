package io.quorumfs.storage;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.Random;

/** Separate JVM fixture. Failure hooks are package-private and never exposed through the CLI. */
public final class StorageProcess {
  private StorageProcess() {}

  public static void main(String[] args) throws Exception {
    Path directory = Path.of(args[0]), signal = Path.of(args[2]);
    boolean tombstone = args[1].startsWith("tombstone-");
    boolean causal = args[1].startsWith("causal-") || tombstone;
    String mode = args[1].replace("causal-", "").replace("tombstone-", "");
    ObjectStorage.Faults hook =
        point -> {
          if (mode.equals(point.name())) {
            try {
              ready(signal, "ready");
              Thread.sleep(120_000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new java.io.IOException(e);
            }
          }
        };
    long size =
        mode.equals("bounded") || mode.equals("disk-full")
            ? ObjectStorage.MAX_OBJECT_BYTES
            : tombstone ? 0 : 4L * ObjectStorage.CHUNK_BYTES;
    byte[] chunk = new byte[ObjectStorage.CHUNK_BYTES];
    new Random(20260930).nextBytes(chunk);
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (long offset = 0; offset < size; offset += chunk.length) digest.update(chunk);
    boolean ioFailure = false;
    try (var store =
        new ObjectStorage(
            directory,
            "process-fixture",
            ObjectStorage.Limits.defaults(),
            hook,
            System::nanoTime)) {
      if (mode.startsWith("HINT_")) {
        store.configureRing(CausalStorageTest.ring(), "node1");
        java.util.UUID id = java.util.UUID.randomUUID();
        ready(signal.resolveSibling(signal.getFileName() + ".version"), id.toString());
        long hintSize = mode.equals("HINT_BOUNDED") ? ObjectStorage.MAX_OBJECT_BYTES : chunk.length;
        MessageDigest hintHash = MessageDigest.getInstance("SHA-256");
        for (long offset = 0; offset < hintSize; offset += chunk.length) hintHash.update(chunk);
        byte[] expected = hintHash.digest();
        int copies = mode.equals("HINT_BOUNDED") ? 4 : 1;
        for (int i = 0; i < copies; i++) {
          java.util.UUID version = i == 0 ? id : java.util.UUID.randomUUID();
          try (var input = repeated(chunk, hintSize)) {
            store.enqueueHint(
                version, new byte[] {42}, java.util.List.of("node2"), hintSize, expected, input);
          }
          store.readHint(version, OutputStream.nullOutputStream());
        }
        if (mode.equals("HINT_BOUNDED")) {
          try (var input = repeated(chunk, hintSize)) {
            try {
              store.enqueueHint(
                  java.util.UUID.randomUUID(),
                  new byte[] {42},
                  java.util.List.of("node2"),
                  hintSize,
                  expected,
                  input);
              throw new AssertionError("Byte budget not enforced");
            } catch (StorageException e) {
              if (e.code() != StorageException.Code.CAPACITY) throw e;
            }
          }
          System.out.println(
              "PASS HINT_BOUNDED maxHeap="
                  + Runtime.getRuntime().maxMemory()
                  + " pinnedBytes="
                  + store.hintStats().bytes());
          return;
        }
        if (mode.equals("HINT_ACK") || mode.equals("HINT_ACKNOWLEDGED"))
          store.acknowledgeHint(id, "node2");
        ready(signal, "ready");
        Thread.sleep(120_000);
        return;
      }
      if (causal) store.configureRing(CausalStorageTest.ring(), "node1");
      byte[] hash = digest.digest();
      var upload =
          tombstone
              ? store.beginReplica(
                  "test",
                  new byte[] {1},
                  0,
                  hash,
                  io.quorumfs.versioning.VectorClock.of(java.util.Map.of("node1", 1L)),
                  CausalStorageTest.ring(),
                  java.util.UUID.randomUUID(),
                  true)
              : causal
                  ? store.beginCausal(
                      "test",
                      new byte[] {1},
                      size,
                      hash,
                      io.quorumfs.versioning.VectorClock.empty())
                  : store.begin("test", new byte[] {1}, size, hash);
      ready(signal.resolveSibling(signal.getFileName() + ".version"), upload.id().toString());
      try {
        for (long offset = 0; offset < size; offset += chunk.length)
          upload.append(offset, chunk, ObjectStorage.sha256(chunk));
        if (mode.equals("STAGED")) {
          ready(signal, "ready");
          Thread.sleep(120_000);
        }
        upload.commit();
        if (mode.equals("ACKNOWLEDGED")) {
          ready(signal, "acknowledged");
          Thread.sleep(120_000);
        }
        if (mode.equals("disk-full")) throw new AssertionError("Expected native disk exhaustion");
        store.read("test", new byte[] {1}, upload.id(), OutputStream.nullOutputStream());
      } catch (StorageException e) {
        if (!mode.equals("disk-full")
            || e.code() != StorageException.Code.IO_FAILURE
            || !(e.getCause() instanceof org.rocksdb.RocksDBException nativeError)
            || nativeError.getStatus().getCode() != org.rocksdb.Status.Code.IOError
            || nativeError.getStatus().getSubCode() != org.rocksdb.Status.SubCode.NoSpace) throw e;
        System.out.println(
            "Native failure: "
                + nativeError.getStatus().getCodeString()
                + " "
                + nativeError.getStatus().getState()
                + " acknowledged=false");
        ioFailure = true;
        ready(signal, nativeError.getStatus().getCodeString());
      }
    }
    if (mode.equals("disk-full") && !ioFailure) throw new AssertionError("No native I/O error");
    System.out.println(
        "PASS " + mode + " maxHeap=" + Runtime.getRuntime().maxMemory() + " objectBytes=" + size);
  }

  private static java.io.InputStream repeated(byte[] chunk, long size) {
    return new java.io.InputStream() {
      private long position;

      @Override
      public int read() {
        return position == size ? -1 : Byte.toUnsignedInt(chunk[(int) (position++ % chunk.length)]);
      }

      @Override
      public int read(byte[] bytes, int offset, int length) {
        if (position == size) return -1;
        int count =
            (int)
                Math.min(length, Math.min(size - position, chunk.length - position % chunk.length));
        System.arraycopy(chunk, (int) (position % chunk.length), bytes, offset, count);
        position += count;
        return count;
      }
    };
  }

  private static void ready(Path path, String value) throws java.io.IOException {
    try (FileChannel file =
        FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      file.write(ByteBuffer.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      file.force(true);
    }
  }
}
