package io.quorumfs.replication;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Channels are keyed by canonical node identity; transport failures never become acknowledgments.
 */
public final class PeerClient implements AutoCloseable {
  public static final Metadata.Key<String> TOKEN =
      Metadata.Key.of("quorumfs-peer-token", Metadata.ASCII_STRING_MARSHALLER);
  private final Map<String, ManagedChannel> channels;
  private final HashRing ring;
  private final String token;

  public PeerClient(Map<String, ManagedChannel> channels, HashRing ring, String token) {
    this.channels = Map.copyOf(channels);
    this.ring = ring;
    this.token = token;
    if (!channels.keySet().equals(ring.nodeIds()))
      throw new IllegalArgumentException("Incomplete peer map");
  }

  private Channel channel(String node) {
    Metadata metadata = new Metadata();
    metadata.put(TOKEN, token);
    return ClientInterceptors.intercept(
        channels.get(node), MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  public ReplicaAck put(String node, ReplicaHeader header, Spool data, long end) throws Exception {
    try (InputStream input = data.input()) {
      var stub =
          ReplicaStoreGrpc.newStub(channel(node))
              .withWaitForReady()
              .withDeadlineAfter(
                  Math.min(Streams.remaining(end), TimeUnit.SECONDS.toNanos(3)),
                  TimeUnit.NANOSECONDS);
      ReplicaAck ack =
          Streams.upload(
              stub::replicateVersion,
              new Streams.Source<ReplicaFrame>() {
                private boolean first = true;
                private long offset;

                public ReplicaFrame next() throws IOException {
                  if (first) {
                    first = false;
                    return ReplicaFrame.newBuilder().setHeader(header).build();
                  }
                  byte[] bytes = input.readNBytes(Wire.CHUNK);
                  if (bytes.length == 0) return null;
                  var frame =
                      ReplicaFrame.newBuilder()
                          .setChunk(
                              ObjectChunk.newBuilder()
                                  .setOffset(offset)
                                  .setData(ByteString.copyFrom(bytes))
                                  .setSha256(Wire.hash(bytes)))
                          .build();
                  offset += bytes.length;
                  return frame;
                }
              },
              end);
      Wire.compatible(ack.getCluster(), ring);
      if (!ack.getDurable()
          || !ack.getNodeId().equals(node)
          || !ack.getVersionId().equals(header.getVersion().getVersionId()))
        throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      return ack;
    }
  }

  public ReplicaVersions read(String node, ObjectKey key, long end) {
    var response =
        ReplicaStoreGrpc.newBlockingStub(channel(node))
            .withWaitForReady()
            .withDeadlineAfter(
                Math.min(Streams.remaining(end), TimeUnit.SECONDS.toNanos(3)), TimeUnit.NANOSECONDS)
            .readVersions(
                ReplicaReadRequest.newBuilder()
                    .setCluster(Wire.identity(ring))
                    .setObject(key)
                    .build());
    Wire.compatible(response.getCluster(), ring);
    if (!response.getNodeId().equals(node) || response.getVersionsCount() > 32)
      throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
    var merged = Wire.merge(response.getVersionsList(), ring);
    if (merged.size() != response.getVersionsCount())
      throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
    return response;
  }

  public Spool fetch(String node, ObjectKey key, VersionMetadata version, Path directory, long end)
      throws Exception {
    Spool spool = new Spool(directory, version.getSize(), version.getSha256().toByteArray());
    boolean success = false;
    try {
      var frames =
          ReplicaStoreGrpc.newBlockingStub(channel(node))
              .withWaitForReady()
              .withDeadlineAfter(
                  Math.min(Streams.remaining(end), TimeUnit.SECONDS.toNanos(3)),
                  TimeUnit.NANOSECONDS)
              .fetchVersion(
                  FetchVersionRequest.newBuilder()
                      .setCluster(Wire.identity(ring))
                      .setObject(key)
                      .setVersionId(version.getVersionId())
                      .build());
      if (!frames.hasNext()) throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      var first = frames.next();
      if (!first.hasHeader()
          || !first.getHeader().getObject().equals(key)
          || !first.getHeader().getVersion().equals(version))
        throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      Wire.compatible(first.getHeader().getCluster(), ring);
      while (frames.hasNext()) {
        Streams.remaining(end);
        var frame = frames.next();
        if (!frame.hasChunk()) throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
        spool.append(frame.getChunk());
      }
      spool.finish();
      success = true;
      return spool;
    } finally {
      if (!success) spool.close();
    }
  }

  @Override
  public void close() {
    channels.values().forEach(ManagedChannel::shutdownNow);
    for (var channel : channels.values()) {
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }
}
