package io.quorumfs.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Versioned, length-prefixed object keys and fixed-width upload/chunk keys. */
final class StorageKeys {
  private StorageKeys() {}

  static byte[] id(UUID id) {
    return ByteBuffer.allocate(17)
        .put((byte) 1)
        .putLong(id.getMostSignificantBits())
        .putLong(id.getLeastSignificantBits())
        .array();
  }

  static UUID id(byte[] bytes) {
    if (bytes.length != 17 || bytes[0] != 1)
      throw new IllegalArgumentException("Invalid upload key encoding");
    ByteBuffer b = ByteBuffer.wrap(bytes);
    b.get();
    return new UUID(b.getLong(), b.getLong());
  }

  static byte[] chunk(UUID id, long offset) {
    return ByteBuffer.allocate(25).put(id(id)).putLong(offset).array();
  }

  static UUID manifestId(byte[] key) {
    if (key == null || key.length < 27 || key[0] != 1)
      throw new IllegalArgumentException("Invalid manifest key");
    ByteBuffer b = ByteBuffer.wrap(key);
    b.get();
    int ns = b.getInt();
    if (ns < 1 || ns > 64 || b.remaining() < ns + 4 + 17)
      throw new IllegalArgumentException("Invalid namespace encoding");
    b.position(b.position() + ns);
    int size = b.getInt();
    if (size < 1 || size > 1024 || b.remaining() != size + 16)
      throw new IllegalArgumentException("Invalid object key encoding");
    b.position(b.position() + size);
    return new UUID(b.getLong(), b.getLong());
  }

  static byte[] manifest(String namespace, byte[] key, UUID version) {
    byte[] ns = namespace.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(1 + 4 + ns.length + 4 + key.length + 16)
        .put((byte) 1)
        .putInt(ns.length)
        .put(ns)
        .putInt(key.length)
        .put(key)
        .putLong(version.getMostSignificantBits())
        .putLong(version.getLeastSignificantBits())
        .array();
  }
}
