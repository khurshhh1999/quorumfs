package io.quorumfs.versioning;

import java.nio.charset.StandardCharsets;
import java.util.*;

/** Immutable, canonical vector; absent components are zero. No wall-clock ordering. */
public record VectorClock(SortedMap<String, Long> counters) {
  public enum Relation {
    BEFORE,
    EQUAL,
    AFTER,
    CONCURRENT
  }

  public VectorClock {
    if (counters.size() > 5) throw new IllegalArgumentException("Vector exceeds fixed membership");
    for (var entry : counters.entrySet()) {
      if (!entry.getKey().matches("[a-zA-Z0-9_-]{1,64}") || entry.getValue() < 1)
        throw new IllegalArgumentException("Invalid vector component");
    }
    TreeMap<String, Long> canonical = new TreeMap<>();
    canonical.putAll(counters);
    counters = Collections.unmodifiableSortedMap(canonical);
  }

  public static VectorClock empty() {
    return new VectorClock(new TreeMap<>());
  }

  public static VectorClock of(Map<String, Long> counters) {
    return new VectorClock(new TreeMap<>(counters));
  }

  public long get(String node) {
    return counters.getOrDefault(node, 0L);
  }

  public VectorClock with(String node, long value) {
    if (value <= get(node)) throw new IllegalArgumentException("Component must increase");
    TreeMap<String, Long> next = new TreeMap<>(counters);
    next.put(node, value);
    return new VectorClock(next);
  }

  public VectorClock merge(VectorClock other) {
    TreeMap<String, Long> result = new TreeMap<>(counters);
    other.counters.forEach((node, value) -> result.merge(node, value, Math::max));
    return new VectorClock(result);
  }

  public Relation compare(VectorClock other) {
    Set<String> nodes = new HashSet<>(counters.keySet());
    nodes.addAll(other.counters.keySet());
    boolean less = false, greater = false;
    for (String node : nodes) {
      less |= get(node) < other.get(node);
      greater |= get(node) > other.get(node);
    }
    return less
        ? (greater ? Relation.CONCURRENT : Relation.BEFORE)
        : (greater ? Relation.AFTER : Relation.EQUAL);
  }

  public void requireMembers(Set<String> members) {
    if (!members.containsAll(counters.keySet()))
      throw new IllegalArgumentException("Unknown vector member");
  }

  public byte[] serialize() {
    StringBuilder text = new StringBuilder("quorumfs-vector-v1\n");
    counters.forEach((node, value) -> text.append(node).append('=').append(value).append('\n'));
    return text.toString().getBytes(StandardCharsets.US_ASCII);
  }

  public static VectorClock deserialize(byte[] bytes) {
    if (bytes == null || bytes.length > 512)
      throw new IllegalArgumentException("Invalid vector encoding");
    String[] lines = new String(bytes, StandardCharsets.US_ASCII).split("\n", -1);
    if (lines.length < 2 || lines.length > 7 || !lines[0].equals("quorumfs-vector-v1"))
      throw new IllegalArgumentException("Invalid vector encoding");
    TreeMap<String, Long> values = new TreeMap<>();
    for (int i = 1; i < lines.length - 1; i++) {
      String[] component = lines[i].split("=", -1);
      if (component.length != 2 || values.put(component[0], Long.parseLong(component[1])) != null)
        throw new IllegalArgumentException("Invalid vector component");
    }
    VectorClock clock = new VectorClock(values);
    if (!Arrays.equals(bytes, clock.serialize()))
      throw new IllegalArgumentException("Noncanonical vector");
    return clock;
  }
}
