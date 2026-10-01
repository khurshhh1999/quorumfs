package io.quorumfs.server;

import io.quorumfs.storage.ObjectStorage;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.UUID;

/** Offline local storage tool. Exclusive DB lock prevents use against a running node. */
public final class StorageMain {
  private StorageMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException(
          "Usage: server storage <config> <put|get|checkpoint|restore|verify> [arguments]");
    NodeConfig config = NodeConfig.load(Path.of(args[0]));
    String command = args[1];
    int length =
        switch (command) {
          case "put" -> 5;
          case "get" -> 6;
          case "checkpoint", "restore" -> 3;
          case "verify" -> 2;
          default -> throw new IllegalArgumentException("Unknown storage command");
        };
    if (args.length != length)
      throw new IllegalArgumentException("Invalid storage command arguments");
    if (command.equals("restore")) {
      ObjectStorage.restore(Path.of(args[2]), config.dataDir(), config.identity());
      System.out.println("RESTORED");
      return;
    }
    try (ObjectStorage store = new ObjectStorage(config.dataDir(), config.identity())) {
      switch (command) {
        case "put" -> {
          Path file = Path.of(args[4]);
          if (!Files.isRegularFile(file))
            throw new IllegalArgumentException("Input must be a regular file");
          long size = Files.size(file);
          if (size > ObjectStorage.MAX_OBJECT_BYTES)
            throw new IllegalArgumentException("Object exceeds 64 MiB");
          MessageDigest digest = MessageDigest.getInstance("SHA-256");
          try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[ObjectStorage.CHUNK_BYTES];
            int n;
            long hashed = 0;
            while ((n = input.read(buffer)) != -1) {
              hashed += n;
              if (hashed > size)
                throw new IllegalArgumentException("Input size changed while hashing");
              digest.update(buffer, 0, n);
            }
            if (hashed != size)
              throw new IllegalArgumentException("Input size changed while hashing");
          }
          try (var upload =
                  store.begin(
                      args[2], args[3].getBytes(StandardCharsets.UTF_8), size, digest.digest());
              InputStream input = Files.newInputStream(file)) {
            long offset = 0;
            byte[] chunk;
            while ((chunk = input.readNBytes(ObjectStorage.CHUNK_BYTES)).length != 0) {
              upload.append(offset, chunk, ObjectStorage.sha256(chunk));
              offset += chunk.length;
            }
            var version = upload.commit();
            System.out.printf("%s %d %d%n", version.id(), version.sequence(), version.size());
          }
        }
        case "get" -> {
          Path target = Path.of(args[5]).toAbsolutePath().normalize();
          if (Files.exists(target)) throw new IllegalArgumentException("Output already exists");
          Path temporary = Files.createTempFile(target.getParent(), ".quorumfs-download-", ".tmp");
          try {
            try (var output = Files.newOutputStream(temporary)) {
              store.read(
                  args[2],
                  args[3].getBytes(StandardCharsets.UTF_8),
                  UUID.fromString(args[4]),
                  output);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
          } finally {
            Files.deleteIfExists(temporary);
          }
          System.out.println("VERIFIED");
        }
        case "checkpoint" -> {
          store.checkpoint(Path.of(args[2]));
          System.out.println("CHECKPOINTED");
        }
        case "verify" -> System.out.printf("VERIFIED %d versions%n", store.verifyAll());
        default -> throw new AssertionError(command);
      }
    }
  }
}
