package io.quorumfs.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.OutputStream;
import java.nio.file.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag("storage-process")
class StorageProcessTest {
  @TempDir Path root;

  private Process start(Path database, String mode, Path signal, Path log) throws Exception {
    return new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx24m",
            "-XX:MaxDirectMemorySize=16m",
            "-cp",
            System.getProperty("storage.test.classpath"),
            StorageProcess.class.getName(),
            database.toString(),
            mode,
            signal.toString())
        .redirectErrorStream(true)
        .redirectOutput(log.toFile())
        .start();
  }

  @Test
  void realProcessKillsBeforeAndAfterPublicationPreserveTheCommitBoundary() throws Exception {
    boundaries(false, false);
  }

  @Test
  void causalMetadataAndHeadIndexShareTheCrashCommitBoundary() throws Exception {
    boundaries(true, false);
  }

  private void boundaries(boolean causal, boolean tombstone) throws Exception {
    for (String mode :
        new String[] {"STAGED", "WAL_SYNC", "MANIFEST_WRITE", "AFTER_MANIFEST", "ACKNOWLEDGED"}) {
      Path directory = root.resolve(mode),
          signal = root.resolve(mode + ".ready"),
          log = root.resolve(mode + ".log");
      Process process =
          start(
              directory,
              tombstone ? "tombstone-" + mode : causal ? "causal-" + mode : mode,
              signal,
              log);
      try {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!Files.exists(signal) && process.isAlive() && System.nanoTime() < deadline)
          Thread.sleep(20);
        assertTrue(Files.exists(signal), () -> read(log));
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        UUID id =
            UUID.fromString(
                Files.readString(signal.resolveSibling(signal.getFileName() + ".version")));
        boolean published = mode.equals("AFTER_MANIFEST") || mode.equals("ACKNOWLEDGED");
        try (var store = new ObjectStorage(directory, "process-fixture")) {
          assertEquals(published ? 1 : 0, store.verifyAll(), mode);
          if (published) store.read("test", new byte[] {1}, id, OutputStream.nullOutputStream());
          else
            assertEquals(
                StorageException.Code.NOT_FOUND,
                assertThrows(StorageException.class, () -> store.head("test", new byte[] {1}, id))
                    .code());
          if (published)
            assertEquals(tombstone, store.head("test", new byte[] {1}, id).tombstone());
          if (causal) {
            store.configureRing(CausalStorageTest.ring(), "node1");
            assertEquals(published ? 1 : 0, store.siblings("test", new byte[] {1}).size());
            if (published) assertEquals(1, store.vector(id).get("node1"));
            try (var next =
                store.beginCausal(
                    "test",
                    new byte[] {1},
                    0,
                    ObjectStorage.sha256(new byte[0]),
                    io.quorumfs.versioning.VectorClock.empty())) {
              assertEquals(tombstone ? 3 : 2, store.vector(next.commit().id()).get("node1"));
            }
          } else assertEquals(2, ObjectStorageTest.put(store, new byte[0]).sequence());
        }
        System.out.println(
            "PASS process causal="
                + causal
                + " boundary="
                + mode
                + " published="
                + published
                + " seed=20260930");
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(10, TimeUnit.SECONDS);
        }
      }
    }
  }

  @Test
  void streamsMaximumObjectWithHeapSmallerThanTheObject() throws Exception {
    Path log = root.resolve("bounded.log");
    Process process = start(root.resolve("bounded"), "bounded", root.resolve("unused.ready"), log);
    try {
      assertTrue(process.waitFor(90, TimeUnit.SECONDS), "Bounded-memory child timed out");
      assertEquals(0, process.exitValue(), () -> read(log));
      assertTrue(read(log).contains("PASS bounded"), () -> read(log));
      System.out.println(read(log));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  @Test
  void tombstoneRepairSharesAtomicCrashPublicationBoundary() throws Exception {
    boundaries(true, true);
  }

  @Test
  void durableHintPublicationAndAcknowledgmentSurviveRealProcessKills() throws Exception {
    for (String mode :
        new String[] {"HINT_PUBLISH", "HINT_DURABLE", "HINT_ACK", "HINT_ACKNOWLEDGED"}) {
      Path database = root.resolve(mode),
          signal = root.resolve(mode + ".ready"),
          log = root.resolve(mode + ".log");
      Process process = start(database, mode, signal, log);
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!Files.exists(signal) && process.isAlive() && System.nanoTime() < deadline)
          Thread.sleep(20);
        assertTrue(Files.exists(signal), () -> read(log));
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        try (var store = new ObjectStorage(database, "process-fixture")) {
          boolean pending = mode.equals("HINT_DURABLE") || mode.equals("HINT_ACK");
          assertEquals(pending ? 1 : 0, store.hints().size(), mode);
          assertEquals(0, store.verifyAll());
          if (pending)
            store.readHint(store.hints().getFirst().id(), OutputStream.nullOutputStream());
        }
      } finally {
        if (process.isAlive()) {
          process.destroyForcibly();
          process.waitFor(10, TimeUnit.SECONDS);
        }
      }
    }
  }

  @Test
  void maximumHintPayloadAndByteBudgetWorkWithTwentyFourMiBHeap() throws Exception {
    Path log = root.resolve("hint-bounded.log");
    Process process =
        start(root.resolve("hint-bounded"), "HINT_BOUNDED", root.resolve("hint.ready"), log);
    try {
      assertTrue(process.waitFor(90, TimeUnit.SECONDS), "Hint memory fixture timed out");
      assertEquals(0, process.exitValue(), () -> read(log));
      assertTrue(read(log).contains("PASS HINT_BOUNDED"), () -> read(log));
      System.out.println(read(log));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
      }
    }
  }

  private static String read(Path path) {
    try {
      return Files.readString(path);
    } catch (java.io.IOException e) {
      return e.toString();
    }
  }
}
