package io.quorumfs.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IdentityStoreTest {
  @TempDir Path directory;

  @Test
  void nativeDatabaseRetainsIdentityAndRejectsChanges() throws Exception {
    try (var first = new IdentityStore(directory, "node1/epoch1")) {
      assertNotNull(first);
    }
    try (var reopened = new IdentityStore(directory, "node1/epoch1")) {
      assertNotNull(reopened);
    }
    assertThrows(IOException.class, () -> new IdentityStore(directory, "node2/epoch1"));
    try (var afterFailure = new IdentityStore(directory, "node1/epoch1")) {
      assertNotNull(afterFailure);
    }
  }

  @Test
  void simultaneousVolumeUseFails() throws Exception {
    try (var first = new IdentityStore(directory, "node1")) {
      assertNotNull(first);
      assertThrows(org.rocksdb.RocksDBException.class, () -> new IdentityStore(directory, "node1"));
    }
  }
}
