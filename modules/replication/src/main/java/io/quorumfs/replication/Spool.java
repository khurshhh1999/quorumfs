package io.quorumfs.replication;

import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;

/** Bounded, private, temporary disk stream. Never a durable replica acknowledgment. */
public final class Spool implements AutoCloseable {
  private final Path path;
  private final OutputStream output;
  private final long size;
  private final byte[] expected;
  private final MessageDigest hash = Wire.digest();
  private long received;
  private boolean finished;

  public Spool(Path directory, long size, byte[] expected) throws IOException {
    Wire.content(size, com.google.protobuf.ByteString.copyFrom(expected));
    this.size = size;
    this.expected = expected.clone();
    path = Files.createTempFile(directory, "transfer-", ".part");
    output = Files.newOutputStream(path);
  }

  public void append(ObjectChunk chunk) throws IOException {
    if (finished
        || chunk.getOffset() != received
        || chunk.getData().isEmpty()
        || chunk.getData().size() != Math.min(Wire.CHUNK, size - received))
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    byte[] data = chunk.getData().toByteArray();
    if (!Wire.hash(data).equals(chunk.getSha256()))
      throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
    output.write(data);
    hash.update(data);
    received += data.length;
  }

  public void finish() throws IOException {
    if (finished || received != size || !MessageDigest.isEqual(hash.digest(), expected))
      throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
    output.close();
    finished = true;
  }

  public InputStream input() throws IOException {
    if (!finished) throw new IllegalStateException("Incomplete spool");
    return Files.newInputStream(path);
  }

  @Override
  public void close() throws IOException {
    try {
      output.close();
    } finally {
      Files.deleteIfExists(path);
    }
  }

  public static Path prepare(Path directory) throws IOException {
    Files.createDirectories(directory);
    try (var files = Files.list(directory)) {
      for (Path file : files.toList()) {
        if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)
            || !file.getFileName().toString().matches("transfer-.*\\.part"))
          throw new IOException("Unexpected transfer directory entry");
        Files.delete(file);
      }
    }
    return directory;
  }
}
