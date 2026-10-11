package io.quorumfs.replication;

import com.google.protobuf.ByteString;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.storage.ObjectStorage;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/** Fixed 16-leaf binary tree over the complete unsigned 128-bit token space. */
public final class Merkle {
  public static final int DEPTH = 4;
  private static final BigInteger MOD = BigInteger.ONE.shiftLeft(128);

  private Merkle() {}

  private static ByteString token(BigInteger value) {
    byte[] encoded = value.mod(MOD).toByteArray();
    byte[] result = new byte[16];
    int count = Math.min(encoded.length, 16);
    System.arraycopy(encoded, encoded.length - count, result, 16 - count, count);
    return ByteString.copyFrom(result);
  }

  public static TokenRange range(int depth, int index) {
    if (depth < 0 || depth > DEPTH || index < 0 || index >= (1 << depth))
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    BigInteger width = MOD.shiftRight(depth);
    return TokenRange.newBuilder()
        .setStartExclusive(
            token(width.multiply(BigInteger.valueOf(index)).subtract(BigInteger.ONE)))
        .setEndInclusive(
            token(width.multiply(BigInteger.valueOf(index + 1L)).subtract(BigInteger.ONE)))
        .build();
  }

  public static int index(TokenRange range, int depth) {
    if (depth < 0 || depth > DEPTH) throw Errors.exception(ErrorReason.INVALID_REQUEST);
    for (int i = 0; i < (1 << depth); i++) if (range.equals(range(depth, i))) return i;
    throw Errors.exception(ErrorReason.INVALID_REQUEST);
  }

  public static int leaf(ObjectKey key) {
    return Integer.parseInt(
        HashRing.keyHash(key.getNamespace(), key.getKey().toByteArray()).substring(0, 1), 16);
  }

  public static VersionMetadata metadata(ObjectStorage.InventoryEntry entry) {
    var version = entry.version();
    return VersionMetadata.newBuilder()
        .setVersionId(version.id().toString())
        .setVector(Wire.vector(entry.vector()))
        .setSize(version.size())
        .setSha256(ByteString.copyFrom(version.sha256()))
        .setTombstone(version.tombstone())
        .build();
  }

  public static String identity(RangeVersion entry) {
    // Hex preserves byte ordering; lengths make namespace/key boundaries unambiguous.
    return HexFormat.of()
            .formatHex(
                entry.getObject().getNamespace().getBytes(java.nio.charset.StandardCharsets.UTF_8))
        + ":"
        + HexFormat.of().formatHex(entry.getObject().getKey().toByteArray())
        + ":"
        + entry.getVersion().getVersionId();
  }

  public static List<RangeVersion> entries(List<ObjectStorage.InventoryEntry> inventory) {
    return inventory.stream()
        .map(
            e ->
                RangeVersion.newBuilder()
                    .setObject(
                        ObjectKey.newBuilder()
                            .setNamespace(e.namespace())
                            .setKey(ByteString.copyFrom(e.key())))
                    .setVersion(metadata(e))
                    .setCorrupt(!e.healthy())
                    .build())
        .sorted(Comparator.comparing(Merkle::identity))
        .toList();
  }

  public static List<MerkleNode> tree(List<RangeVersion> entries, int depth) {
    if (depth < 0 || depth > DEPTH) throw Errors.exception(ErrorReason.INVALID_REQUEST);
    List<ByteString> hashes = new ArrayList<>();
    for (int leaf = 0; leaf < 16; leaf++) {
      var digest = Wire.digest();
      digest.update((byte) 0);
      digest.update(range(DEPTH, leaf).toByteArray());
      final int bucket = leaf;
      entries.stream()
          .filter(e -> leaf(e.getObject()) == bucket)
          .sorted(Comparator.comparing(Merkle::identity))
          .forEach(
              e -> {
                byte[] object = e.getObject().toByteArray();
                byte[] vector =
                    io.quorumfs.versioning.VectorClock.of(
                            e.getVersion().getVector().getCountersMap())
                        .serialize();
                byte[] id =
                    e.getVersion()
                        .getVersionId()
                        .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                for (byte[] bytes :
                    List.of(object, id, vector, e.getVersion().getSha256().toByteArray())) {
                  digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
                  digest.update(bytes);
                }
                digest.update(ByteBuffer.allocate(8).putLong(e.getVersion().getSize()).array());
                digest.update((byte) (e.getVersion().getTombstone() ? 1 : 0));
                digest.update((byte) (e.getCorrupt() ? 1 : 0));
              });
      hashes.add(ByteString.copyFrom(digest.digest()));
    }
    for (int level = DEPTH; level > depth; level--) {
      List<ByteString> parents = new ArrayList<>();
      for (int i = 0; i < hashes.size(); i += 2) {
        var digest = Wire.digest();
        digest.update((byte) 1);
        digest.update(hashes.get(i).toByteArray());
        digest.update(hashes.get(i + 1).toByteArray());
        parents.add(ByteString.copyFrom(digest.digest()));
      }
      hashes = parents;
    }
    List<MerkleNode> result = new ArrayList<>();
    for (int i = 0; i < hashes.size(); i++)
      result.add(
          MerkleNode.newBuilder().setRange(range(depth, i)).setSha256(hashes.get(i)).build());
    return List.copyOf(result);
  }
}
