package io.quorumfs.server;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class NodeConfigTest {
  private final List<NodeConfig.Member> members =
      java.util.stream.IntStream.rangeClosed(1, 5)
          .mapToObj(i -> new NodeConfig.Member("node" + i, "localhost:" + (19000 + i)))
          .toList();

  private NodeConfig config(int n, int r, int w, List<NodeConfig.Member> m) {
    return new NodeConfig("test", 1, "node1", 19001, Path.of("data"), m, n, r, w);
  }

  @Test
  void validatesAllQuorumCombinations() {
    System.out.println("seed=" + System.getProperty("quorumfs.seed"));
    for (int n = 0; n <= 6; n++)
      for (int r = 0; r <= 6; r++)
        for (int w = 0; w <= 6; w++) {
          int nn = n, rr = r, ww = w;
          boolean valid = n >= 1 && n <= 5 && r >= 1 && r <= n && w >= 1 && w <= n && r + w > n;
          if (valid) assertDoesNotThrow(() -> config(nn, rr, ww, members));
          else assertThrows(IllegalArgumentException.class, () -> config(nn, rr, ww, members));
        }
  }

  @Test
  void rejectsDuplicateIdsAndEndpoints() {
    var changed = new java.util.ArrayList<>(members);
    changed.set(4, new NodeConfig.Member("node1", "localhost:19009"));
    assertThrows(IllegalArgumentException.class, () -> config(3, 2, 2, changed));
    changed.set(4, new NodeConfig.Member("node5", "localhost:19001"));
    assertThrows(IllegalArgumentException.class, () -> config(3, 2, 2, changed));
  }

  @Test
  void requiresFiveMembersAndLocalIdentity() {
    assertThrows(IllegalArgumentException.class, () -> config(3, 2, 2, members.subList(0, 4)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new NodeConfig("test", 1, "absent", 19001, Path.of("data"), members, 3, 2, 2));
  }
}
