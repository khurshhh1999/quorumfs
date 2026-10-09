package io.quorumfs.replication;

import com.google.protobuf.ByteString;
import io.grpc.stub.*;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.storage.*;
import java.io.*;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

public final class ReplicaService extends ReplicaStoreGrpc.ReplicaStoreImplBase {
  private final ObjectStorage store;
  private final HashRing ring;
  private final String node;
  private final Path transfers;
  private final Semaphore slots = new Semaphore(8);

  public ReplicaService(ObjectStorage store, HashRing ring, String node, Path transfers) {
    this.store = store;
    this.ring = ring;
    this.node = node;
    this.transfers = transfers;
  }

  private void check(ClusterIdentity identity, ObjectKey key) {
    Wire.compatible(identity, ring);
    Wire.key(key);
    if (!ring.owners(key.getNamespace(), key.getKey().toByteArray()).contains(node))
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
  }

  private VersionMetadata metadata(ObjectKey key, String id) throws IOException {
    var value = store.head(key.getNamespace(), key.getKey().toByteArray(), Wire.uuid(id));
    return VersionMetadata.newBuilder()
        .setVersionId(id)
        .setVector(Wire.vector(store.vector(value.id())))
        .setSize(value.size())
        .setTombstone(value.tombstone())
        .setSha256(ByteString.copyFrom(value.sha256()))
        .build();
  }

  @Override
  public StreamObserver<ReplicaFrame> replicateVersion(StreamObserver<ReplicaAck> response) {
    try {
      return new UploadObserver<>(response, slots) {
        private ReplicaHeader header;

        @Override
        protected void frame(ReplicaFrame frame) throws Exception {
          if (header == null) {
            if (!frame.hasHeader()) throw Errors.exception(ErrorReason.INVALID_REQUEST);
            header = frame.getHeader();
            check(header.getCluster(), header.getObject());
            Wire.request(header.getRequestId());
            Wire.version(header.getVersion(), ring);
            spool =
                new Spool(
                    transfers,
                    header.getVersion().getSize(),
                    header.getVersion().getSha256().toByteArray());
          } else {
            if (!frame.hasChunk()) throw Errors.exception(ErrorReason.INVALID_REQUEST);
            spool.append(frame.getChunk());
          }
        }

        @Override
        protected ReplicaAck complete() throws Exception {
          return ingest(header, spool, end);
        }
      };
    } catch (RuntimeException e) {
      response.onError(RpcFailure.map(e));
      return ignored();
    }
  }

  private ReplicaAck ingest(ReplicaHeader header, Spool spool, long end) throws Exception {
    // Serialize duplicate replay against foreground publication, including the existence check.
    synchronized (store) {
      ObjectKey key = header.getObject();
      VersionMetadata version = header.getVersion();
      boolean exists = false;
      try {
        VersionMetadata previous = metadata(key, version.getVersionId());
        if (!previous.equals(version)) throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
        store.read(
            key.getNamespace(),
            key.getKey().toByteArray(),
            Wire.uuid(version.getVersionId()),
            OutputStream.nullOutputStream());
        exists = true;
      } catch (StorageException e) {
        if (e.code() != StorageException.Code.NOT_FOUND) throw e;
      }
      if (!exists) {
        try (var upload =
                store.beginReplica(
                    key.getNamespace(),
                    key.getKey().toByteArray(),
                    version.getSize(),
                    version.getSha256().toByteArray(),
                    Wire.vector(version.getVector(), ring),
                    ring,
                    Wire.uuid(version.getVersionId()),
                    version.getTombstone());
            var input = spool.input()) {
          byte[] bytes;
          long offset = 0;
          while ((bytes = input.readNBytes(Wire.CHUNK)).length != 0) {
            Streams.remaining(end);
            upload.append(offset, bytes, Wire.hash(bytes).toByteArray());
            offset += bytes.length;
          }
          Streams.remaining(end);
          upload.commit();
        }
      }
      return ReplicaAck.newBuilder()
          .setCluster(Wire.identity(ring))
          .setNodeId(node)
          .setVersionId(version.getVersionId())
          .setDurable(true)
          .build();
    }
  }

  @Override
  public StreamObserver<ReplicaFrame> deliverHint(StreamObserver<ReplicaAck> response) {
    return replicateVersion(response);
  }

  @Override
  public void applyTombstone(TombstoneRequest request, StreamObserver<ReplicaAck> response) {
    if (!slots.tryAcquire()) {
      response.onError(Errors.exception(ErrorReason.CAPACITY_EXHAUSTED));
      return;
    }
    try {
      var header = request.getHeader();
      check(header.getCluster(), header.getObject());
      Wire.request(header.getRequestId());
      Wire.version(header.getVersion(), ring);
      if (!header.getVersion().getTombstone()) throw Errors.exception(ErrorReason.INVALID_REQUEST);
      try (var spool = new Spool(transfers, 0, Wire.hash(new byte[0]).toByteArray())) {
        spool.finish();
        response.onNext(ingest(header, spool, Streams.end()));
        response.onCompleted();
      }
    } catch (Exception e) {
      response.onError(RpcFailure.map(e));
    } finally {
      slots.release();
    }
  }

  public static <T> StreamObserver<T> ignored() {
    return new StreamObserver<>() {
      public void onNext(T value) {}

      public void onError(Throwable error) {}

      public void onCompleted() {}
    };
  }

  @Override
  public void readVersions(ReplicaReadRequest request, StreamObserver<ReplicaVersions> response) {
    if (!slots.tryAcquire()) {
      response.onError(Errors.exception(ErrorReason.CAPACITY_EXHAUSTED));
      return;
    }
    try {
      check(request.getCluster(), request.getObject());
      var key = request.getObject();
      var result = ReplicaVersions.newBuilder().setCluster(Wire.identity(ring)).setNodeId(node);
      for (var entry : store.siblings(key.getNamespace(), key.getKey().toByteArray())) {
        Streams.remaining(Streams.end());
        store.read(
            key.getNamespace(),
            key.getKey().toByteArray(),
            entry.id(),
            OutputStream.nullOutputStream());
        result.addVersions(metadata(key, entry.id().toString()));
      }
      response.onNext(result.build());
      response.onCompleted();
    } catch (Exception e) {
      response.onError(RpcFailure.map(e));
    } finally {
      slots.release();
    }
  }

  @Override
  public void fetchVersion(FetchVersionRequest request, StreamObserver<ReplicaFrame> response) {
    if (!slots.tryAcquire()) {
      response.onError(Errors.exception(ErrorReason.CAPACITY_EXHAUSTED));
      return;
    }
    try {
      long end = Streams.end();
      check(request.getCluster(), request.getObject());
      var key = request.getObject();
      var version = metadata(key, request.getVersionId());
      var outbound = (ServerCallStreamObserver<ReplicaFrame>) response;
      Streams.send(
          outbound,
          ReplicaFrame.newBuilder()
              .setHeader(
                  ReplicaHeader.newBuilder()
                      .setCluster(Wire.identity(ring))
                      .setObject(key)
                      .setVersion(version))
              .build(),
          end);
      store.read(
          key.getNamespace(),
          key.getKey().toByteArray(),
          Wire.uuid(version.getVersionId()),
          new OutputStream() {
            private long offset;

            @Override
            public void write(int b) {
              throw new UnsupportedOperationException();
            }

            @Override
            public void write(byte[] bytes, int start, int length) throws IOException {
              byte[] data = java.util.Arrays.copyOfRange(bytes, start, start + length);
              try {
                Streams.send(
                    outbound,
                    ReplicaFrame.newBuilder()
                        .setChunk(
                            ObjectChunk.newBuilder()
                                .setOffset(offset)
                                .setData(ByteString.copyFrom(data))
                                .setSha256(Wire.hash(data)))
                        .build(),
                    end);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
              }
              offset += length;
            }
          });
      response.onCompleted();
    } catch (Exception e) {
      response.onError(RpcFailure.map(e));
    } finally {
      slots.release();
    }
  }
}
