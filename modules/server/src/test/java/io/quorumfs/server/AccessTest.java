package io.quorumfs.server;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import org.junit.jupiter.api.Test;

class AccessTest {
  @Test
  void separatesPeerAndClientCredentialsAndBoundsNamespacePolicy() {
    String client = "c".repeat(32), peer = "p".repeat(32);
    assertEquals(Set.of("demo"), new Access(client, peer, Set.of("demo")).namespaces());
    assertThrows(IllegalArgumentException.class, () -> new Access(client, client, Set.of("demo")));
    assertThrows(IllegalArgumentException.class, () -> new Access("short", peer, Set.of("demo")));
    assertThrows(IllegalArgumentException.class, () -> new Access(client, peer, Set.of()));
    assertThrows(IllegalArgumentException.class, () -> new Access(client, peer, Set.of("*")));
    assertThrows(
        IllegalArgumentException.class, () -> new Access(client, peer, Set.of("bad/namespace")));
  }
}
