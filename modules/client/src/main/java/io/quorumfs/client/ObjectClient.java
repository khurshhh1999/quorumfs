package io.quorumfs.client;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.versioning.VectorClock;
import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Context-aware client with bounded streams and verified, exclusively published downloads. */
public final class ObjectClient implements AutoCloseable {
  private final ManagedChannel channel;
  private final Channel authorized;
  private final Duration timeout;

  public ObjectClient(ManagedChannel channel, String token, Duration timeout) {
    this.channel = channel;
    this.timeout = timeout;
    if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofSeconds(15)) > 0)
      throw new IllegalArgumentException("Timeout must be >0 and <=15 seconds");
    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("quorumfs-client-token", Metadata.ASCII_STRING_MARSHALLER), token);
    authorized =
        ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  public static ObjectKey key(String namespace, String key) {
    return ObjectKey.newBuilder()
        .setNamespace(namespace)
        .setKey(ByteString.copyFromUtf8(key))
        .build();
  }

  public List<VersionMetadata> head(ObjectKey key) {
    return ObjectStoreGrpc.newBlockingStub(authorized)
        .withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
        .headObject(HeadObjectRequest.newBuilder().setObject(key).build())
        .getVersionsList();
  }

  public WriteResult put(ObjectKey key, Path file, VectorClock context, boolean resolve)
      throws Exception {
    if (!Files.isRegularFile(file))
      throw new IllegalArgumentException("Input must be a regular file");
    long size = Files.size(file);
    if (size > Wire.MAX_SIZE) throw new IllegalArgumentException("Maximum object size is 64 MiB");
    MessageDigest hash = Wire.digest();
    try (InputStream input = Files.newInputStream(file)) {
      byte[] buffer = new byte[Wire.CHUNK];
      int n;
      long received = 0;
      while ((n = input.read(buffer)) != -1) {
        received += n;
        if (received > size) throw new IOException("Input size changed");
        hash.update(buffer, 0, n);
      }
      if (received != size) throw new IOException("Input size changed");
    }
    WriteHeader header =
        WriteHeader.newBuilder()
            .setObject(key)
            .setRequestId(UUID.randomUUID().toString())
            .setContext(Wire.vector(context))
            .setSize(size)
            .setSha256(ByteString.copyFrom(hash.digest()))
            .build();
    long end = System.nanoTime() + timeout.toNanos();
    try (InputStream input = Files.newInputStream(file)) {
      var stub =
          ObjectStoreGrpc.newStub(authorized)
              .withDeadlineAfter(Streams.remaining(end), TimeUnit.NANOSECONDS);
      return Streams.upload(
          resolve ? stub::resolveObject : stub::putObject,
          new Streams.Source<PutObjectRequest>() {
            private boolean first = true;
            private long offset;

            public PutObjectRequest next() throws IOException {
              if (first) {
                first = false;
                return PutObjectRequest.newBuilder().setHeader(header).build();
              }
              byte[] data = input.readNBytes(Wire.CHUNK);
              if (data.length == 0) return null;
              var frame =
                  PutObjectRequest.newBuilder()
                      .setChunk(
                          ObjectChunk.newBuilder()
                              .setOffset(offset)
                              .setData(ByteString.copyFrom(data))
                              .setSha256(Wire.hash(data)))
                      .build();
              offset += data.length;
              return frame;
            }
          },
          end);
    }
  }

  public VersionMetadata get(ObjectKey key, String version, Path output) throws Exception {
    Path target = output.toAbsolutePath();
    if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Output exists");
    Path temp = Files.createTempFile(target.getParent(), ".quorumfs-download-", ".tmp");
    try {
      var frames =
          ObjectStoreGrpc.newBlockingStub(authorized)
              .withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
              .getObject(
                  GetObjectRequest.newBuilder().setObject(key).setVersionId(version).build());
      if (!frames.hasNext()) throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      var first = frames.next();
      if (first.hasConflict()) throw Errors.exception(ErrorReason.CONFLICT);
      if (!first.hasHeader()) throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      var header = first.getHeader();
      Wire.content(header.getSize(), header.getSha256());
      MessageDigest hash = Wire.digest();
      long received = 0;
      try (OutputStream sink = Files.newOutputStream(temp)) {
        while (frames.hasNext()) {
          var frame = frames.next();
          if (!frame.hasChunk()) throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
          var chunk = frame.getChunk();
          byte[] data = chunk.getData().toByteArray();
          if (chunk.getOffset() != received
              || data.length == 0
              || data.length != Math.min(Wire.CHUNK, header.getSize() - received)
              || !Wire.hash(data).equals(chunk.getSha256()))
            throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
          sink.write(data);
          hash.update(data);
          received += data.length;
        }
      }
      if (received != header.getSize()
          || !MessageDigest.isEqual(hash.digest(), header.getSha256().toByteArray()))
        throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      Files.createLink(target, temp); // Atomic, exclusive publication on the same filesystem.
      return header;
    } finally {
      Files.deleteIfExists(temp);
    }
  }

  @Override
  public void close() {
    channel.shutdownNow();
    try {
      channel.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
