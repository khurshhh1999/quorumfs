package io.quorumfs.versioning;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;

class VectorClockTest {
  private VectorClock random(Random random) {
    TreeMap<String, Long> map = new TreeMap<>();
    for (int i = 1; i <= 5; i++) {
      long value = random.nextInt(16);
      if (value > 0) map.put("node" + i, value);
    }
    return VectorClock.of(map);
  }

  @Test
  void seededVectorAlgebra() {
    Random random = new Random(20261006);
    for (int i = 0; i < 10000; i++) {
      VectorClock a = random(random), b = random(random), c = random(random);
      assertEquals(a, a.merge(a));
      assertEquals(a.merge(b), b.merge(a));
      assertEquals(a.merge(b).merge(c), a.merge(b.merge(c)));
      assertEquals(a, VectorClock.deserialize(a.serialize()));
      assertTrue(
          Set.of(VectorClock.Relation.EQUAL, VectorClock.Relation.BEFORE)
              .contains(a.compare(a.merge(b))));
      if (a.compare(b) == VectorClock.Relation.BEFORE)
        assertEquals(VectorClock.Relation.AFTER, b.compare(a));
      if (a.compare(b) == VectorClock.Relation.CONCURRENT)
        assertEquals(VectorClock.Relation.CONCURRENT, b.compare(a));
      if (a.compare(b) == VectorClock.Relation.BEFORE
          && b.compare(c) == VectorClock.Relation.BEFORE)
        assertEquals(VectorClock.Relation.BEFORE, a.compare(c));
    }
  }

  @Test
  void siblingMergeConvergesIndependentOfOrderAndBatching() {
    Random random = new Random(20261006);
    for (int iteration = 0; iteration < 500; iteration++) {
      List<Siblings.Entry> versions = new ArrayList<>();
      for (int i = 0; i < 20; i++)
        versions.add(new Siblings.Entry(new UUID(0, i), random(random), "a".repeat(64)));
      List<Siblings.Entry> expected = Siblings.merge(versions);
      Collections.shuffle(versions, random);
      List<Siblings.Entry> merged = List.of();
      for (var version : versions) {
        List<Siblings.Entry> next = new ArrayList<>(merged);
        next.add(version);
        merged = Siblings.merge(next);
      }
      assertEquals(expected, merged);
      assertEquals(expected, Siblings.merge(expected));
      VectorClock resolved = Siblings.context(versions).with("node1", 100);
      for (var version : versions)
        assertEquals(VectorClock.Relation.AFTER, resolved.compare(version.clock()));
    }
  }

  @Test
  void integrityBoundsAndCanonicalDecoding() {
    VectorClock a = VectorClock.of(Map.of("a", 1L));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            Siblings.merge(
                List.of(
                    new Siblings.Entry(new UUID(0, 1), a, "a".repeat(64)),
                    new Siblings.Entry(new UUID(0, 2), a, "b".repeat(64)))));
    assertThrows(IllegalArgumentException.class, () -> VectorClock.of(Map.of("a", 0L)));
    assertThrows(IllegalArgumentException.class, () -> a.requireMembers(Set.of("b")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            VectorClock.deserialize(
                "quorumfs-vector-v1\na=01\n".getBytes(StandardCharsets.US_ASCII)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            VectorClock.deserialize(
                "quorumfs-vector-v1\na=1\na=2\n".getBytes(StandardCharsets.US_ASCII)));
    List<Siblings.Entry> many = new ArrayList<>();
    for (int i = 1; i <= 33; i++)
      many.add(
          new Siblings.Entry(
              new UUID(0, i), VectorClock.of(Map.of("a", (long) i, "b", 34L - i)), "a".repeat(64)));
    assertThrows(IllegalStateException.class, () -> Siblings.merge(many));
  }

  @Test
  void publicConstructorNormalizesCustomComparatorsAndEqualVectorsDeduplicate() {
    TreeMap<String, Long> reversed = new TreeMap<>(Comparator.reverseOrder());
    reversed.put("node1", 1L);
    reversed.put("node2", 2L);
    VectorClock vector = new VectorClock(reversed);
    assertArrayEquals(
        VectorClock.of(Map.of("node1", 1L, "node2", 2L)).serialize(), vector.serialize());
    reversed.clear();
    assertEquals(2, vector.counters().size());
    var first = new Siblings.Entry(new UUID(0, 1), vector, "a".repeat(64));
    var duplicate = new Siblings.Entry(new UUID(0, 2), vector, "a".repeat(64));
    assertEquals(List.of(first), Siblings.merge(List.of(duplicate, first)));
    assertEquals(List.of(first), Siblings.merge(List.of(first, duplicate)));
  }
}
