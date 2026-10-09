package io.quorumfs.versioning;

import java.util.*;

/** Deterministic maximal antichain; equal causal identities must have identical content. */
public final class Siblings {
  public static final int LIMIT = 32;

  private Siblings() {}

  public record Entry(UUID id, VectorClock clock, String digest, boolean tombstone) {
    public Entry(UUID id, VectorClock clock, String digest) {
      this(id, clock, digest, false);
    }

    public Entry {
      Objects.requireNonNull(id);
      Objects.requireNonNull(clock);
      if (clock.counters().isEmpty() || !digest.matches("[0-9a-f]{64}"))
        throw new IllegalArgumentException("Invalid causal version");
    }
  }

  public static List<Entry> merge(Collection<Entry> versions) {
    List<Entry> all = List.copyOf(versions);
    for (Entry a : all)
      for (Entry b : all) {
        if ((a.clock().equals(b.clock())
                && (!a.digest().equals(b.digest()) || a.tombstone() != b.tombstone()))
            || (a.id().equals(b.id()) && !a.equals(b)))
          throw new IllegalArgumentException("Causal identity has conflicting content");
      }
    List<Entry> result =
        all.stream()
            .distinct()
            .filter(
                a ->
                    all.stream()
                        .noneMatch(
                            b ->
                                a.clock().compare(b.clock()) == VectorClock.Relation.BEFORE
                                    || (a.clock().equals(b.clock())
                                        && a.id().toString().compareTo(b.id().toString()) > 0)))
            .sorted(Comparator.comparing(a -> a.id().toString()))
            .toList();
    if (result.size() > LIMIT) throw new IllegalStateException("Sibling limit exceeded");
    return result;
  }

  public static VectorClock context(Collection<Entry> observed) {
    VectorClock context = VectorClock.empty();
    for (Entry entry : observed) context = context.merge(entry.clock());
    return context;
  }
}
