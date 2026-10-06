package io.quorumfs.server;

import io.quorumfs.storage.ObjectStorage;
import io.quorumfs.versioning.VectorClock;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;

/** Offline local storage tool. Exclusive DB lock prevents use against a running node. */
public final class StorageMain {
  private StorageMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      throw new IllegalArgumentException(
          "Usage: server storage <config> <put|put-causal|resolve|siblings|owners|get|checkpoint|restore|verify> [arguments]");
    NodeConfig config = NodeConfig.load(Path.of(args[0]));
    String command = args[1];
    int length =
        switch (command) {
          case "put" -> 5;
          case "get", "put-causal", "resolve" -> 6;
          case "siblings", "owners" -> 4;
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
      store.configureRing(config.ring(), config.nodeId());
      switch (command) {
        case "owners" ->
            System.out.printf(
                "%s %s%n",
                config.ring().fingerprint(),
                String.join(
                    ",", config.ring().owners(args[2], args[3].getBytes(StandardCharsets.UTF_8))));
        case "siblings" -> {
          for (var sibling : store.siblings(args[2], args[3].getBytes(StandardCharsets.UTF_8)))
            System.out.printf(
                "%s %s %s%n",
                sibling.id(),
                sibling.digest(),
                HexFormat.of().formatHex(sibling.clock().serialize()));
        }
        case "put", "put-causal", "resolve" -> {
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
          try (var upload = begin(store, command, args, size, digest.digest());
              InputStream input = Files.newInputStream(file)) {
            long offset = 0;
            byte[] chunk;
            while ((chunk = input.readNBytes(ObjectStorage.CHUNK_BYTES)).length != 0) {
              upload.append(offset, chunk, ObjectStorage.sha256(chunk));
              offset += chunk.length;
            }
            var version = upload.commit();
            System.out.printf("%s %d %d", version.id(), version.sequence(), version.size());
            if (!command.equals("put"))
              System.out.print(
                  " " + HexFormat.of().formatHex(store.vector(version.id()).serialize()));
            System.out.println();
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

  private static ObjectStorage.Upload begin(
      ObjectStorage store, String command, String[] args, long size, byte[] digest)
      throws Exception {
    byte[] key = args[3].getBytes(StandardCharsets.UTF_8);
    return switch (command) {
      case "put" -> store.begin(args[2], key, size, digest);
      case "put-causal" ->
          store.beginCausal(
              args[2],
              key,
              size,
              digest,
              args[5].equals("-")
                  ? VectorClock.empty()
                  : VectorClock.deserialize(HexFormat.of().parseHex(args[5])));
      case "resolve" ->
          store.resolve(
              args[2],
              key,
              size,
              digest,
              Arrays.stream(args[5].split(",", -1)).map(UUID::fromString).toList());
      default -> throw new AssertionError(command);
    };
  }
}
