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
    boolean causal = args[1].startsWith("causal-");
    String mode = args[1].replace("causal-", "");
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
            : 4L * ObjectStorage.CHUNK_BYTES;
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
      if (causal) store.configureRing(CausalStorageTest.ring(), "node1");
      byte[] hash = digest.digest();
      var upload =
          causal
              ? store.beginCausal(
                  "test", new byte[] {1}, size, hash, io.quorumfs.versioning.VectorClock.empty())
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

  private static void ready(Path path, String value) throws java.io.IOException {
    try (FileChannel file =
        FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      file.write(ByteBuffer.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
      file.force(true);
    }
  }
}
