package io.quorumfs.ring;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;

class HashRingTest {
  static List<HashRing.Member> members() {
    return java.util.stream.IntStream.rangeClosed(1, 5)
        .mapToObj(i -> new HashRing.Member("node" + i, "localhost:" + (9000 + i)))
        .toList();
  }

  static HashRing ring(List<HashRing.Member> members, int n) {
    return new HashRing("test", 1, members, n, n, 1);
  }

  @Test
  void fiveIndependentNodesAgreeAcrossPermutationsAndReplicaCounts() {
    Random random = new Random(20261006);
    for (int n = 1; n <= 5; n++) {
      HashRing baseline = ring(members(), n);
      for (int node = 0; node < 5; node++) {
        List<HashRing.Member> shuffled = new ArrayList<>(members());
        Collections.shuffle(shuffled, random);
        HashRing other = HashRing.deserialize(ring(shuffled, n).serialize());
        assertArrayEquals(baseline.serialize(), other.serialize());
        for (int i = 0; i < 2000; i++) {
          byte[] key = new byte[32];
          random.nextBytes(key);
          assertEquals(baseline.owners("ns", key), other.owners("ns", key));
          assertEquals(n, new HashSet<>(other.owners("ns", key)).size());
        }
      }
    }
  }

  @Test
  void rejectsInvalidEpochMembershipQuorumAndSameEpochDrift() {
    HashRing original = ring(members(), 3);
    assertThrows(IllegalArgumentException.class, () -> new HashRing("test", 0, members(), 3, 2, 2));
    assertThrows(
        IllegalArgumentException.class,
        () -> original.requireCompatible(new HashRing("test", 2, members(), 3, 3, 1)));
    assertThrows(
        IllegalArgumentException.class,
        () -> original.requireCompatible(new HashRing("test", 1, members(), 3, 2, 2)));
    List<HashRing.Member> changed = new ArrayList<>(members());
    changed.set(0, new HashRing.Member("node1", "other:9001"));
    assertThrows(
        IllegalArgumentException.class, () -> original.requireCompatible(ring(changed, 3)));
    changed.set(0, changed.get(1));
    assertThrows(IllegalArgumentException.class, () -> ring(changed, 3));
    assertThrows(IllegalArgumentException.class, () -> new HashRing("test", 1, members(), 3, 1, 1));
  }

  @Test
  void serializationIsDefensiveAndDetectsVnodeTampering() {
    HashRing ring = ring(members(), 3);
    byte[] raw = ring.serialize();
    raw[raw.length - 3] ^= 1;
    assertThrows(IllegalArgumentException.class, () -> HashRing.deserialize(raw));
    ring.requireCompatible(HashRing.deserialize(ring.serialize()));
    assertNotEquals(
        HashRing.keyHash("ab", new byte[] {'c'}), HashRing.keyHash("a", new byte[] {'b', 'c'}));
    assertThrows(IllegalArgumentException.class, () -> ring.owners("ns", new byte[0]));
  }

  @Test
  void goldenHashAndOwners() {
    assertEquals(
        "cb9355e0e9c04e4546023b6f3fca5293",
        HashRing.keyHash("ns", "key".getBytes(StandardCharsets.UTF_8)));
    assertEquals(
        List.of("node1", "node4", "node3"),
        ring(members(), 3).owners("ns", "key".getBytes(StandardCharsets.UTF_8)));
  }
}
