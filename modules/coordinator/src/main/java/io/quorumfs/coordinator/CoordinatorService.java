package io.quorumfs.coordinator;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.*;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.replication.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.storage.ObjectStorage;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/** Strict canonical-owner quorums. Temporary coordinator bytes never count toward W. */
public final class CoordinatorService extends ObjectStoreGrpc.ObjectStoreImplBase
    implements AutoCloseable {
  private final ObjectStorage store;
  private final HashRing ring;
  private final PeerClient peers;
  private final Recovery recovery;
  private final ClusterInfo info;
  private final Path transfers;
  private final Set<String> namespaces;
  private final Semaphore slots = new Semaphore(8);
  private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

  public CoordinatorService(
      ObjectStorage store,
      HashRing ring,
      PeerClient peers,
      ClusterInfo info,
      Path transfers,
      Set<String> namespaces) {
    this.store = store;
    this.ring = ring;
    this.peers = peers;
    this.info = info;
    this.transfers = transfers;
    this.namespaces = Set.copyOf(namespaces);
    recovery = new Recovery(store, peers, ring, transfers);
  }

  private void authorize(ObjectKey key) {
    Wire.key(key);
    if (!namespaces.contains(key.getNamespace())) throw Errors.exception(ErrorReason.ACCESS_DENIED);
  }

  @FunctionalInterface
  private interface Operation<T> {
    T run(String node) throws Exception;
  }

  private record Reply<T>(String node, T value) {}

  private <T> List<Reply<T>> quorum(
      ObjectKey key, int required, long end, boolean write, Operation<T> operation) {
    List<String> owners = ring.owners(key.getNamespace(), key.getKey().toByteArray());
    CompletionService<Reply<T>> completed = new ExecutorCompletionService<>(workers);
    List<Future<Reply<T>>> pending = new ArrayList<>();
    List<Reply<T>> accepted = new ArrayList<>();
    Context.CancellableContext context = Context.current().withCancellation();
    RuntimeException last = null;
    try {
      for (String node : owners)
        pending.add(completed.submit(context.wrap(() -> new Reply<>(node, operation.run(node)))));
      for (int received = 0; received < owners.size(); received++) {
        Future<Reply<T>> result = completed.poll(Streams.remaining(end), TimeUnit.NANOSECONDS);
        if (result == null) break;
        try {
          accepted.add(result.get());
        } catch (ExecutionException e) {
          RuntimeException candidate = RpcFailure.map(e);
          if (last == null
              || Set.of(Status.Code.DATA_LOSS, Status.Code.FAILED_PRECONDITION)
                  .contains(Status.fromThrowable(candidate).getCode())) last = candidate;
        }
        if (accepted.size() >= required) return List.copyOf(accepted);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      last = e;
    } finally {
      context.cancel(null);
      pending.forEach(f -> f.cancel(true));
    }
    if (write) throw Errors.exception(ErrorReason.OUTCOME_UNKNOWN);
    if (last != null
        && Set.of(Status.Code.DATA_LOSS, Status.Code.FAILED_PRECONDITION)
            .contains(Status.fromThrowable(last).getCode())) throw last;
    throw Errors.exception(ErrorReason.QUORUM_UNAVAILABLE);
  }

  @Override
  public void getClusterInfo(GetClusterInfoRequest request, StreamObserver<ClusterInfo> response) {
    response.onNext(info);
    response.onCompleted();
  }

  @Override
  public StreamObserver<PutObjectRequest> putObject(StreamObserver<WriteResult> response) {
    return upload(response, false);
  }

  @Override
  public StreamObserver<PutObjectRequest> resolveObject(StreamObserver<WriteResult> response) {
    return upload(response, true);
  }

  private StreamObserver<PutObjectRequest> upload(
      StreamObserver<WriteResult> response, boolean resolve) {
    try {
      return new UploadObserver<>(response, slots) {
        private WriteHeader header;

        @Override
        protected void frame(PutObjectRequest frame) throws Exception {
          if (header == null) {
            if (!frame.hasHeader()) throw Errors.exception(ErrorReason.INVALID_REQUEST);
            header = frame.getHeader();
            authorize(header.getObject());
            Wire.request(header.getRequestId());
            Wire.content(header.getSize(), header.getSha256());
            var context = Wire.vector(header.getContext(), ring);
            if (resolve && context.counters().isEmpty())
              throw Errors.exception(ErrorReason.INVALID_REQUEST);
            spool = new Spool(transfers, header.getSize(), header.getSha256().toByteArray());
          } else {
            if (!frame.hasChunk()) throw Errors.exception(ErrorReason.INVALID_REQUEST);
            spool.append(frame.getChunk());
          }
        }

        @Override
        protected WriteResult complete() throws Exception {
          return publish(header, spool, false, end);
        }
      };
    } catch (RuntimeException e) {
      response.onError(RpcFailure.map(e));
      return ReplicaService.ignored();
    }
  }

  private WriteResult publish(WriteHeader header, Spool spool, boolean tombstone, long end)
      throws Exception {
    // This read preflight avoids allocation when a quorum is already known unavailable.
    var observed =
        quorum(
            header.getObject(),
            info.getWriteQuorum(),
            end,
            false,
            node -> peers.read(node, header.getObject(), end));
    var vector = store.allocateVector(Wire.vector(header.getContext(), ring));
    var version =
        VersionMetadata.newBuilder()
            .setVersionId(UUID.randomUUID().toString())
            .setVector(Wire.vector(vector))
            .setSize(header.getSize())
            .setTombstone(tombstone)
            .setSha256(header.getSha256())
            .build();
    List<VersionMetadata> candidates = new ArrayList<>();
    for (var reply : observed) candidates.addAll(reply.value().getVersionsList());
    candidates.add(version);
    Wire.merge(candidates, ring); // Detect known sibling exhaustion before sending any version.
    var replica =
        ReplicaHeader.newBuilder()
            .setCluster(Wire.identity(ring))
            .setObject(header.getObject())
            .setVersion(version)
            .setRequestId(header.getRequestId())
            .build();
    recovery.persist(
        replica,
        spool,
        ring.owners(header.getObject().getNamespace(), header.getObject().getKey().toByteArray()));
    var acknowledgments =
        quorum(
            header.getObject(),
            info.getWriteQuorum(),
            end,
            true,
            node -> {
              var ack = peers.put(node, replica, spool, end);
              store.acknowledgeHint(Wire.uuid(version.getVersionId()), node);
              return ack;
            });
    System.out.printf(
        "event=quorum_write node=%s durable_owners=%d%n", info.getNodeId(), acknowledgments.size());
    return WriteResult.newBuilder()
        .setVersion(version)
        .setDurableOwnerCount(acknowledgments.size())
        .build();
  }

  @Override
  public void deleteObject(DeleteObjectRequest request, StreamObserver<WriteResult> response) {
    if (!slots.tryAcquire()) {
      response.onError(Errors.exception(ErrorReason.CAPACITY_EXHAUSTED));
      return;
    }
    try {
      authorize(request.getObject());
      Wire.request(request.getRequestId());
      Wire.vector(request.getContext(), ring);
      try (var spool = new Spool(transfers, 0, Wire.hash(new byte[0]).toByteArray())) {
        spool.finish();
        var header =
            WriteHeader.newBuilder()
                .setObject(request.getObject())
                .setRequestId(request.getRequestId())
                .setContext(request.getContext())
                .setSha256(Wire.hash(new byte[0]))
                .build();
        response.onNext(publish(header, spool, true, Streams.end()));
        response.onCompleted();
      }
    } catch (Exception e) {
      response.onError(RpcFailure.map(e));
    } finally {
      slots.release();
    }
  }

  private record Read(List<VersionMetadata> versions, Map<String, List<String>> holders) {}

  private Read read(ObjectKey key, long end) {
    authorize(key);
    var responses =
        quorum(key, info.getReadQuorum(), end, false, node -> peers.read(node, key, end));
    List<VersionMetadata> versions = new ArrayList<>();
    Map<String, List<String>> holders = new HashMap<>();
    for (var response : responses)
      for (var version : response.value().getVersionsList()) {
        versions.add(version);
        holders
            .computeIfAbsent(version.getVersionId(), ignored -> new ArrayList<>())
            .add(response.node());
      }
    var merged = Wire.merge(versions, ring);
    Map<String, List<VersionMetadata>> observed = new HashMap<>();
    for (var reply : responses) observed.put(reply.node(), reply.value().getVersionsList());
    recovery.repair(key, merged, holders, observed);
    return new Read(merged, holders);
  }

  @Override
  public void headObject(HeadObjectRequest request, StreamObserver<HeadObjectResponse> response) {
    if (!slots.tryAcquire()) {
      response.onError(Errors.exception(ErrorReason.CAPACITY_EXHAUSTED));
      return;
    }
    try {
      var read = read(request.getObject(), Streams.end());
      if (read.versions().isEmpty()) throw Errors.exception(ErrorReason.OBJECT_NOT_FOUND);
      response.onNext(HeadObjectResponse.newBuilder().addAllVersions(read.versions()).build());
      response.onCompleted();
    } catch (Exception e) {
      response.onError(RpcFailure.map(e));
    } finally {
      slots.release();
    }
  }

  @Override
  public void getObject(GetObjectRequest request, StreamObserver<GetObjectResponse> response) {
    if (!slots.tryAcquire()) {
      response.onError(Errors.exception(ErrorReason.CAPACITY_EXHAUSTED));
      return;
    }
    try {
      long end = Streams.end();
      if (!request.getVersionId().isEmpty()) Wire.uuid(request.getVersionId());
      var read = read(request.getObject(), end);
      if (read.versions().isEmpty()) throw Errors.exception(ErrorReason.OBJECT_NOT_FOUND);
      if (request.getVersionId().isEmpty() && read.versions().size() > 1) {
        response.onNext(
            GetObjectResponse.newBuilder()
                .setConflict(VersionSet.newBuilder().addAllVersions(read.versions()))
                .build());
        response.onCompleted();
        return;
      }
      var selected =
          request.getVersionId().isEmpty()
              ? read.versions().getFirst()
              : read.versions().stream()
                  .filter(v -> v.getVersionId().equals(request.getVersionId()))
                  .findFirst()
                  .orElseThrow(() -> Errors.exception(ErrorReason.OBJECT_NOT_FOUND));
      if (selected.getTombstone()) throw Errors.exception(ErrorReason.OBJECT_NOT_FOUND);
      Spool data = null;
      Exception last = null;
      for (String holder : read.holders().get(selected.getVersionId())) {
        try {
          data = peers.fetch(holder, request.getObject(), selected, transfers, end);
          break;
        } catch (Exception e) {
          last = e;
        }
      }
      if (data == null)
        throw last == null
            ? Errors.exception(ErrorReason.QUORUM_UNAVAILABLE)
            : RpcFailure.map(last);
      try (Spool complete = data;
          var input = complete.input()) {
        var outbound = (ServerCallStreamObserver<GetObjectResponse>) response;
        Streams.send(outbound, GetObjectResponse.newBuilder().setHeader(selected).build(), end);
        byte[] bytes;
        long offset = 0;
        while ((bytes = input.readNBytes(Wire.CHUNK)).length != 0) {
          Streams.send(
              outbound,
              GetObjectResponse.newBuilder()
                  .setChunk(
                      ObjectChunk.newBuilder()
                          .setOffset(offset)
                          .setData(ByteString.copyFrom(bytes))
                          .setSha256(Wire.hash(bytes)))
                  .build(),
              end);
          offset += bytes.length;
        }
      }
      response.onCompleted();
    } catch (Exception e) {
      response.onError(RpcFailure.map(e));
    } finally {
      slots.release();
    }
  }

  @Override
  public void close() {
    recovery.close();
    workers.shutdownNow();
    workers.close();
    peers.close();
  }
}
