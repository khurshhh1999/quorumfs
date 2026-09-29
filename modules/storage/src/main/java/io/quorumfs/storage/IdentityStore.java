package io.quorumfs.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

/** Q0 identity-only database. This is not an object storage implementation. */
public final class IdentityStore implements AutoCloseable {
  private final Options options;
  private final RocksDB db;
  private static final byte[] KEY = "identity/v1".getBytes(StandardCharsets.UTF_8);

  static {
    RocksDB.loadLibrary();
  }

  public IdentityStore(Path directory, String identity) throws IOException, RocksDBException {
    Files.createDirectories(directory);
    options =
        new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize(8L * 1024 * 1024)
            .setMaxOpenFiles(64);
    RocksDB opened = null;
    try {
      opened = RocksDB.open(options, directory.toString());
      byte[] expected = identity.getBytes(StandardCharsets.UTF_8);
      byte[] existing = opened.get(KEY);
      if (existing == null) {
        try (WriteOptions write = new WriteOptions().setSync(true).setDisableWAL(false)) {
          opened.put(write, KEY, expected);
        }
      } else if (!java.util.Arrays.equals(existing, expected)) {
        throw new IOException(
            "Persisted node/cluster identity or membership differs; refusing startup");
      }
      db = opened;
    } catch (IOException | RocksDBException | RuntimeException e) {
      if (opened != null) opened.close();
      options.close();
      throw e;
    }
  }

  @Override
  public void close() {
    db.close();
    options.close();
  }
}
