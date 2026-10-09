package io.quorumfs.protocol;

import com.google.protobuf.ByteString;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.versioning.*;
import java.security.*;
import java.util.*;

public final class Wire {
  public static final int CHUNK = 256 * 1024;
  public static final long MAX_SIZE = 64L * 1024 * 1024;

  private Wire() {}

  public static MessageDigest digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  public static ByteString hash(byte[] data) {
    return ByteString.copyFrom(digest().digest(data));
  }

  public static void key(ObjectKey key) {
    if (!key.getNamespace().matches("[a-zA-Z0-9_-]{1,64}")
        || key.getKey().isEmpty()
        || key.getKey().size() > 1024) throw Errors.exception(ErrorReason.INVALID_REQUEST);
  }

  public static void content(long size, ByteString digest) {
    if (size < 0 || size > MAX_SIZE || digest.size() != 32)
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
  }

  public static void request(String id) {
    if (!id.matches("[a-zA-Z0-9_-]{1,128}")) throw Errors.exception(ErrorReason.INVALID_REQUEST);
  }

  public static UUID uuid(String id) {
    try {
      UUID value = UUID.fromString(id);
      if (!value.toString().equals(id)) throw new IllegalArgumentException();
      return value;
    } catch (IllegalArgumentException e) {
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    }
  }

  public static VectorClock vector(VersionVector vector, HashRing ring) {
    try {
      VectorClock result = VectorClock.of(vector.getCountersMap());
      result.requireMembers(ring.nodeIds());
      return result;
    } catch (IllegalArgumentException e) {
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    }
  }

  public static VersionVector vector(VectorClock vector) {
    return VersionVector.newBuilder().putAllCounters(vector.counters()).build();
  }

  public static void version(VersionMetadata version, HashRing ring) {
    uuid(version.getVersionId());
    content(version.getSize(), version.getSha256());
    if ((version.getTombstone()
            && (version.getSize() != 0 || !version.getSha256().equals(hash(new byte[0]))))
        || vector(version.getVector(), ring).counters().isEmpty())
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
  }

  public static ClusterIdentity identity(HashRing ring) {
    return ClusterIdentity.newBuilder()
        .setClusterId(ring.cluster())
        .setEpoch(ring.epoch())
        .setRingConfiguration(ByteString.copyFrom(ring.serialize()))
        .build();
  }

  public static void compatible(ClusterIdentity identity, HashRing ring) {
    if (!identity.equals(identity(ring))) throw Errors.exception(ErrorReason.EPOCH_MISMATCH);
  }

  public static List<VersionMetadata> merge(Collection<VersionMetadata> versions, HashRing ring) {
    Map<UUID, VersionMetadata> byId = new HashMap<>();
    List<Siblings.Entry> entries = new ArrayList<>();
    Map<VectorClock, VersionMetadata> byVector = new HashMap<>();
    for (var version : versions) {
      version(version, ring);
      VectorClock causal = vector(version.getVector(), ring);
      VersionMetadata previous = byVector.putIfAbsent(causal, version);
      if (previous != null
          && (previous.getTombstone() != version.getTombstone()
              || previous.getSize() != version.getSize()
              || !previous.getSha256().equals(version.getSha256())))
        throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      UUID id = uuid(version.getVersionId());
      VersionMetadata old = byId.putIfAbsent(id, version);
      if (old != null && !old.equals(version))
        throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      entries.add(
          new Siblings.Entry(
              id,
              vector(version.getVector(), ring),
              HexFormat.of().formatHex(version.getSha256().toByteArray()),
              version.getTombstone()));
    }
    try {
      return Siblings.merge(entries).stream().map(e -> byId.get(e.id())).toList();
    } catch (IllegalArgumentException e) {
      throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
    } catch (IllegalStateException e) {
      throw Errors.exception(ErrorReason.CAPACITY_EXHAUSTED);
    }
  }
}
