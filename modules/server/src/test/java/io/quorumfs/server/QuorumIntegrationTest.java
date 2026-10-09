package io.quorumfs.server;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.*;
import io.grpc.stub.MetadataUtils;
import io.quorumfs.client.ObjectClient;
import io.quorumfs.coordinator.CoordinatorService;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.replication.*;
import io.quorumfs.storage.*;
import io.quorumfs.versioning.VectorClock;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Tag("quorum-integration")
class QuorumIntegrationTest {
  @TempDir Path root;
  private Cluster cluster;

  @BeforeEach
  void start() throws Exception {
    cluster = new Cluster();
  }

  @AfterEach
  void stop() throws Exception {
    if (cluster != null) cluster.close();
  }

  private Path payload(String name, int size) throws IOException {
    byte[] bytes = new byte[size];
    new Random(20261007L + name.hashCode()).nextBytes(bytes);
    return Files.write(root.resolve(name), bytes);
  }

  private Status.Code status(Throwable error) {
    while (error instanceof ExecutionException && error.getCause() != null)
      error = error.getCause();
    return Status.fromThrowable(error).getCode();
  }

  private ObjectKey key() {
    return ObjectClient.key("demo", "object");
  }

  private WriteResult put(String node, ObjectKey key, Path file, VectorClock context)
      throws Exception {
    try (var client = cluster.client(node)) {
      return client.put(key, file, context, false);
    }
  }

  @Test
  void durableCanonicalOwnersAndRestartWithNonOwnerCoordinator() throws Exception {
    var owners = cluster.owners(key());
    String coordinator =
        cluster.ids.stream().filter(n -> !owners.contains(n)).findFirst().orElseThrow();
    Path input = payload("input", 2 * Wire.CHUNK + 1);
    var result = put(coordinator, key(), input, VectorClock.empty());
    assertEquals(2, result.getDurableOwnerCount());
    int copies = 0;
    for (String node : cluster.ids) {
      var visible = cluster.nodes.get(node).store.siblings("demo", key().getKey().toByteArray());
      if (!owners.contains(node))
        assertTrue(visible.isEmpty(), "Non-owner temporary bytes counted/published");
      else if (visible.stream()
          .anyMatch(v -> v.id().toString().equals(result.getVersion().getVersionId()))) copies++;
    }
    assertTrue(copies >= 2);
    String victim =
        owners.stream()
            .filter(
                n -> {
                  try {
                    return !cluster
                        .nodes
                        .get(n)
                        .store
                        .siblings("demo", key().getKey().toByteArray())
                        .isEmpty();
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .findFirst()
            .orElseThrow();
    cluster.nodes.get(victim).close();
    try (var client = cluster.client(coordinator)) {
      client.get(key(), "", root.resolve("with-owner-down"));
      assertEquals(-1L, Files.mismatch(input, root.resolve("with-owner-down")));
    }
    for (String node : cluster.ids) cluster.restart(node);
    try (var client = cluster.client(coordinator)) {
      var version = client.get(key(), "", root.resolve("out"));
      assertEquals(result.getVersion(), version);
      assertArrayEquals(Files.readAllBytes(input), Files.readAllBytes(root.resolve("out")));
      assertThrows(IOException.class, () -> client.get(key(), "", root.resolve("out")));
    }
  }

  @Test
  void realNetworkPartitionAllowsMajorityAndRejectsMinorityWithoutWeakeningQuorum()
      throws Exception {
    var owners = cluster.owners(key());
    Set<String> majority = Set.of(owners.get(0), owners.get(1));
    cluster.partition(majority);
    Path input = payload("partition", 99);
    put(owners.get(0), key(), input, VectorClock.empty());
    try (var client = cluster.client(owners.get(0))) {
      assertEquals(1, client.head(key()).size());
    }
    try (var client = cluster.client(owners.get(2))) {
      assertEquals(
          Status.Code.UNAVAILABLE, status(assertThrows(Exception.class, () -> client.head(key()))));
      assertEquals(
          Status.Code.UNAVAILABLE,
          status(
              assertThrows(
                  Exception.class, () -> client.put(key(), input, VectorClock.empty(), false))));
    }
    cluster.heal();
    try (var client = cluster.client(owners.get(2))) {
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      while (true) {
        try {
          assertEquals(1, client.head(key()).size());
          break;
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() != Status.Code.UNAVAILABLE || System.nanoTime() >= until)
            throw e;
          Thread.sleep(100);
        }
      }
    }
  }

  @Test
  void concurrentWritesExposeConflictsAndExplicitContextResolves() throws Exception {
    Path a = payload("a", 700), b = payload("b", 900);
    try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
      CountDownLatch go = new CountDownLatch(1);
      var first =
          workers.submit(
              () -> {
                go.await();
                return put("node1", key(), a, VectorClock.empty());
              });
      var second =
          workers.submit(
              () -> {
                go.await();
                return put("node2", key(), b, VectorClock.empty());
              });
      go.countDown();
      var x = first.get();
      var y = second.get();
      try (var client = cluster.client("node3")) {
        assertEquals(2, client.head(key()).size());
        assertEquals(
            Status.Code.ABORTED,
            status(
                assertThrows(
                    Exception.class, () -> client.get(key(), "", root.resolve("conflict")))));
        assertFalse(Files.exists(root.resolve("conflict")));
        client.get(key(), x.getVersion().getVersionId(), root.resolve("selected"));
        assertArrayEquals(Files.readAllBytes(a), Files.readAllBytes(root.resolve("selected")));
        VectorClock context =
            VectorClock.of(x.getVersion().getVector().getCountersMap())
                .merge(VectorClock.of(y.getVersion().getVector().getCountersMap()));
        var resolved = client.put(key(), a, context, true);
        assertEquals(List.of(resolved.getVersion()), client.head(key()));
      }
    }
  }

  @Test
  void slowReplicaDoesNotBlockTwoDurableOwners() throws Exception {
    var owners = cluster.owners(key());
    cluster.nodes.get(owners.get(0)).fault = "slow-ack";
    long start = System.nanoTime();
    var result = put("node5", key(), payload("slow", 20), VectorClock.empty());
    assertEquals(2, result.getDurableOwnerCount());
    assertTrue(
        System.nanoTime() - start < TimeUnit.SECONDS.toNanos(3), "Waited for slow third owner");
  }

  @Test
  void invalidAcknowledgmentsNeverCountAndPartialCommitsReportUnknown() throws Exception {
    var owners = cluster.owners(key());
    cluster.disconnect(owners.get(2));
    for (String fault : List.of("wrong-node", "not-durable", "wrong-version", "wrong-epoch")) {
      cluster.nodes.get(owners.get(0)).fault = fault;
      try (var client = cluster.client("node5")) {
        Exception error =
            assertThrows(
                Exception.class,
                () -> client.put(key(), payload(fault, 17), VectorClock.empty(), false));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, status(error), fault);
        Throwable cause = error;
        while (cause instanceof ExecutionException) cause = cause.getCause();
        var trailers = Status.trailersFromThrowable(cause);
        assertNotNull(trailers);
        assertTrue(ErrorDetail.parseFrom(trailers.get(Errors.DETAIL)).getOutcomeUnknown());
      }
    }
    cluster.nodes.get(owners.get(0)).fault = "";
    cluster.heal();
    try (var client = cluster.client("node5")) {
      assertFalse(client.head(key()).isEmpty(), "Unknown outcome may still be committed");
    }
  }

  @Test
  void unknownOutcomeAfterDurableCommitDoesNotLoseTheVersion() throws Exception {
    for (String owner : cluster.owners(key())) cluster.nodes.get(owner).fault = "slow-ack";
    Path input = payload("unknown", 51);
    Exception error =
        assertThrows(Exception.class, () -> put("node4", key(), input, VectorClock.empty()));
    assertEquals(Status.Code.DEADLINE_EXCEEDED, status(error));
    cluster.nodes.values().forEach(n -> n.fault = "");
    try (var client = cluster.client("node4")) {
      client.get(key(), "", root.resolve("recovered"));
      assertArrayEquals(Files.readAllBytes(input), Files.readAllBytes(root.resolve("recovered")));
    }
  }

  @Test
  void authorizationAndEpochFailuresPrecedePublication() throws Exception {
    try (var client = cluster.client("node1", "wrong", 8000)) {
      assertEquals(
          Status.Code.PERMISSION_DENIED,
          status(assertThrows(Exception.class, () -> client.head(key()))));
    }
    try (var client = cluster.client("node1")) {
      assertEquals(
          Status.Code.PERMISSION_DENIED,
          status(
              assertThrows(
                  Exception.class, () -> client.head(ObjectClient.key("private", "key")))));
    }
    String owner = cluster.owners(key()).getFirst();
    ManagedChannel channel = cluster.direct(owner);
    try {
      Metadata publicCredential = new Metadata();
      publicCredential.put(PeerClient.TOKEN, cluster.access.clientToken());
      Channel wrong =
          ClientInterceptors.intercept(
              channel, MetadataUtils.newAttachHeadersInterceptor(publicCredential));
      assertEquals(
          Status.Code.PERMISSION_DENIED,
          status(
              assertThrows(
                  Exception.class,
                  () ->
                      ReplicaStoreGrpc.newBlockingStub(wrong)
                          .readVersions(
                              ReplicaReadRequest.newBuilder()
                                  .setCluster(Wire.identity(cluster.ring()))
                                  .setObject(key())
                                  .build()))));
      Metadata peerCredential = new Metadata();
      peerCredential.put(PeerClient.TOKEN, cluster.access.peerToken());
      Channel authorized =
          ClientInterceptors.intercept(
              channel, MetadataUtils.newAttachHeadersInterceptor(peerCredential));
      var request =
          ReplicaReadRequest.newBuilder()
              .setCluster(Wire.identity(cluster.ring()).toBuilder().setEpoch(2))
              .setObject(key())
              .build();
      assertEquals(
          Status.Code.FAILED_PRECONDITION,
          status(
              assertThrows(
                  Exception.class,
                  () -> ReplicaStoreGrpc.newBlockingStub(authorized).readVersions(request))));
      var sameEpoch =
          request.toBuilder()
              .setCluster(
                  Wire.identity(cluster.ring()).toBuilder()
                      .setRingConfiguration(ByteString.copyFromUtf8("different")))
              .build();
      assertEquals(
          Status.Code.FAILED_PRECONDITION,
          status(
              assertThrows(
                  Exception.class,
                  () -> ReplicaStoreGrpc.newBlockingStub(authorized).readVersions(sameEpoch))));
    } finally {
      channel.shutdownNow();
    }
    for (Node node : cluster.nodes.values()) assertEquals(0, node.store.verifyAll());
  }

  @Test
  void corruptAndTruncatedUploadsNeverAcknowledgeAndSlotsAreReleased() throws Exception {
    ManagedChannel channel = cluster.direct("node1");
    try {
      Metadata auth = new Metadata();
      auth.put(Access.CLIENT_TOKEN, cluster.access.clientToken());
      Channel authorized =
          ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(auth));
      byte[] data = {1};
      var header =
          WriteHeader.newBuilder()
              .setObject(key())
              .setRequestId("test")
              .setSize(1)
              .setSha256(Wire.hash(data))
              .build();
      for (int i = 0; i < 10; i++) {
        var frames =
            List.of(
                    PutObjectRequest.newBuilder().setHeader(header).build(),
                    PutObjectRequest.newBuilder()
                        .setChunk(
                            ObjectChunk.newBuilder()
                                .setData(ByteString.copyFrom(data))
                                .setSha256(Wire.hash(new byte[] {2})))
                        .build())
                .iterator();
        var stub = ObjectStoreGrpc.newStub(authorized).withDeadlineAfter(2, TimeUnit.SECONDS);
        assertEquals(
            Status.Code.DATA_LOSS,
            status(
                assertThrows(
                    Exception.class,
                    () ->
                        Streams.upload(
                            stub::putObject,
                            () -> frames.hasNext() ? frames.next() : null,
                            System.nanoTime() + TimeUnit.SECONDS.toNanos(2)))));
      }
      var oneFrame = List.of(PutObjectRequest.newBuilder().setHeader(header).build()).iterator();
      var stub = ObjectStoreGrpc.newStub(authorized).withDeadlineAfter(2, TimeUnit.SECONDS);
      assertEquals(
          Status.Code.DATA_LOSS,
          status(
              assertThrows(
                  Exception.class,
                  () ->
                      Streams.upload(
                          stub::putObject,
                          () -> oneFrame.hasNext() ? oneFrame.next() : null,
                          System.nanoTime() + TimeUnit.SECONDS.toNanos(2)))));
    } finally {
      channel.shutdownNow();
    }
    put("node1", key(), payload("after-invalid", 0), VectorClock.empty());
  }

  @Test
  void streamsLargeObjectAndMissingReadRequiresQuorum() throws Exception {
    try (var client = cluster.client("node3")) {
      assertEquals(
          Status.Code.NOT_FOUND, status(assertThrows(Exception.class, () -> client.head(key()))));
    }
    Path input = payload("large", 8 * 1024 * 1024 + 1);
    put("node1", key(), input, VectorClock.empty());
    try (var client = cluster.client("node2")) {
      client.get(key(), "", root.resolve("large-out"));
      assertEquals(Files.mismatch(input, root.resolve("large-out")), -1L);
    }
  }

  @Test
  void damagedReplicaFetchNeverPublishesAClientFile() throws Exception {
    put("node1", key(), payload("intact", 27), VectorClock.empty());
    for (String owner : cluster.owners(key())) cluster.nodes.get(owner).fault = "bad-fetch";
    try (var client = cluster.client("node5")) {
      assertEquals(
          Status.Code.DATA_LOSS,
          status(
              assertThrows(
                  Exception.class, () -> client.get(key(), "", root.resolve("bad-output")))));
      assertFalse(Files.exists(root.resolve("bad-output")));
    }
  }

  @Test
  void cancelledUploadsReleaseAdmissionAndTemporaryBytes() throws Exception {
    ManagedChannel channel = cluster.direct("node1");
    try {
      Metadata auth = new Metadata();
      auth.put(Access.CLIENT_TOKEN, cluster.access.clientToken());
      Channel authorized =
          ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(auth));
      var stub = ObjectStoreGrpc.newStub(authorized).withDeadlineAfter(10, TimeUnit.SECONDS);
      List<io.grpc.stub.StreamObserver<PutObjectRequest>> uploads = new ArrayList<>();
      List<CompletableFuture<Throwable>> outcomes = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        outcomes.add(result);
        var upload =
            stub.putObject(
                new io.grpc.stub.StreamObserver<WriteResult>() {
                  public void onNext(WriteResult result) {
                    fail("Incomplete upload acknowledged");
                  }

                  public void onError(Throwable error) {
                    result.complete(error);
                  }

                  public void onCompleted() {
                    result.complete(null);
                  }
                });
        upload.onNext(
            PutObjectRequest.newBuilder()
                .setHeader(
                    WriteHeader.newBuilder()
                        .setObject(key())
                        .setRequestId("idle")
                        .setSize(1)
                        .setSha256(Wire.hash(new byte[] {1})))
                .build());
        uploads.add(upload);
      }
      long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      Path directory = root.resolve("node1/transfers");
      while (true) {
        try (var files = Files.list(directory)) {
          if (files.count() == 8) break;
        }
        if (System.nanoTime() > until) fail("Uploads not admitted");
        Thread.sleep(10);
      }
      try (var client = cluster.client("node1")) {
        assertEquals(
            Status.Code.RESOURCE_EXHAUSTED,
            status(assertThrows(Exception.class, () -> client.head(key()))));
      }
      uploads.forEach(u -> u.onError(Status.CANCELLED.asRuntimeException()));
      for (var result : outcomes) result.get(5, TimeUnit.SECONDS);
      until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (true) {
        try (var files = Files.list(directory)) {
          if (files.findAny().isEmpty()) break;
        }
        if (System.nanoTime() > until) fail("Cancelled temporary files were not removed");
        Thread.sleep(10);
      }
      put("node1", key(), payload("after-cancel", 1), VectorClock.empty());
    } finally {
      channel.shutdownNow();
    }
  }

  @Test
  void replicaReplayPreservesIdentityAndRejectsConflictingContent() throws Exception {
    byte[] bytes = {1};
    Path input = Files.write(root.resolve("replay"), bytes);
    var result = put("node1", key(), input, VectorClock.empty());
    String owner =
        cluster.owners(key()).stream()
            .filter(
                n -> {
                  try {
                    return !cluster
                        .nodes
                        .get(n)
                        .store
                        .siblings("demo", key().getKey().toByteArray())
                        .isEmpty();
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .findFirst()
            .orElseThrow();
    var id = Wire.uuid(result.getVersion().getVersionId());
    long sequence =
        cluster.nodes.get(owner).store.head("demo", key().getKey().toByteArray(), id).sequence();
    Map<String, ManagedChannel> channels = new HashMap<>();
    for (String node : cluster.ids) channels.put(node, cluster.direct(node));
    try (var peer = new PeerClient(channels, cluster.ring(), cluster.access.peerToken());
        var data = new Spool(root, 1, Wire.hash(bytes).toByteArray())) {
      data.append(
          ObjectChunk.newBuilder()
              .setData(ByteString.copyFrom(bytes))
              .setSha256(Wire.hash(bytes))
              .build());
      data.finish();
      var header =
          ReplicaHeader.newBuilder()
              .setCluster(Wire.identity(cluster.ring()))
              .setObject(key())
              .setVersion(result.getVersion())
              .setRequestId("replay")
              .build();
      assertTrue(
          peer.deliver(owner, header, data, System.nanoTime() + TimeUnit.SECONDS.toNanos(5))
              .getDurable());
      assertEquals(
          sequence,
          cluster.nodes.get(owner).store.head("demo", key().getKey().toByteArray(), id).sequence());
      var wrong =
          header.toBuilder()
              .setVersion(
                  header.getVersion().toBuilder()
                      .setVector(VersionVector.newBuilder().putCounters("node5", 99)))
              .build();
      assertEquals(
          Status.Code.DATA_LOSS,
          status(
              assertThrows(
                  Exception.class,
                  () ->
                      peer.put(
                          owner, wrong, data, System.nanoTime() + TimeUnit.SECONDS.toNanos(5)))));
    }
  }

  @Test
  void invalidReadIdentityCannotSatisfyReadQuorum() throws Exception {
    put("node1", key(), payload("read-identity", 2), VectorClock.empty());
    var owners = cluster.owners(key());
    cluster.disconnect(owners.get(2));
    cluster.nodes.get(owners.get(0)).fault = "bad-read";
    try (var client = cluster.client("node4")) {
      assertEquals(
          Status.Code.DATA_LOSS, status(assertThrows(Exception.class, () -> client.head(key()))));
    }
  }

  private void eventually(Callable<Boolean> condition) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
    while (!condition.call()) {
      if (System.nanoTime() > end) fail("Recovery did not converge within 25 seconds");
      Thread.sleep(50);
    }
  }

  @Test
  void offlineOwnerReceivesDurableDeleteAfterCoordinatorRestartWithoutForegroundRead()
      throws Exception {
    var owners = cluster.owners(key());
    String coordinator =
        cluster.ids.stream().filter(n -> !owners.contains(n)).findFirst().orElseThrow();
    var original = put(coordinator, key(), payload("deleted", 101), VectorClock.empty());
    eventually(
        () ->
            owners.stream()
                .allMatch(
                    n -> {
                      try {
                        return !cluster
                            .nodes
                            .get(n)
                            .store
                            .siblings("demo", key().getKey().toByteArray())
                            .isEmpty();
                      } catch (IOException e) {
                        throw new UncheckedIOException(e);
                      }
                    }));
    String offline = owners.get(2);
    cluster.disconnect(offline);
    VersionMetadata tombstone;
    try (var client = cluster.client(coordinator)) {
      var deleted =
          client.delete(key(), Wire.vector(original.getVersion().getVector(), cluster.ring()));
      assertEquals(2, deleted.getDurableOwnerCount());
      tombstone = deleted.getVersion();
      assertTrue(tombstone.getTombstone());
      assertEquals(
          Status.Code.NOT_FOUND,
          status(
              assertThrows(
                  Exception.class, () -> client.get(key(), "", root.resolve("deleted-output")))));
      assertFalse(Files.exists(root.resolve("deleted-output")));
      assertTrue(client.head(key()).getFirst().getTombstone());
    }
    assertTrue(
        cluster.nodes.get(coordinator).store.hints().stream()
            .anyMatch(
                h ->
                    h.id().toString().equals(tombstone.getVersionId())
                        && h.destinations().contains(offline)));
    cluster.restart(coordinator);
    cluster.heal();
    eventually(
        () ->
            cluster.nodes.get(offline).store.siblings("demo", key().getKey().toByteArray()).stream()
                .allMatch(
                    e -> e.id().toString().equals(tombstone.getVersionId()) && e.tombstone()));
    eventually(() -> cluster.nodes.get(coordinator).store.hints().isEmpty());
    cluster.restart(offline);
    assertTrue(
        cluster
            .nodes
            .get(offline)
            .store
            .head("demo", key().getKey().toByteArray(), Wire.uuid(tombstone.getVersionId()))
            .tombstone());
    try (var client = cluster.client(coordinator)) {
      var recreated =
          client.put(
              key(),
              payload("recreated", 1),
              Wire.vector(tombstone.getVector(), cluster.ring()),
              false);
      assertFalse(recreated.getVersion().getTombstone());
      assertEquals(recreated.getVersion(), client.get(key(), "", root.resolve("recreated-output")));
    }
  }

  @Test
  void concurrentDeleteAndWriteRemainConflictAndObservedContextCanResolve() throws Exception {
    var original = put("node1", key(), payload("base-delete", 1), VectorClock.empty());
    var context = Wire.vector(original.getVersion().getVector(), cluster.ring());
    VersionMetadata deleted;
    try (var client = cluster.client("node2")) {
      deleted = client.delete(key(), context).getVersion();
    }
    var live = put("node3", key(), payload("concurrent-live", 2), context).getVersion();
    try (var client = cluster.client("node4")) {
      var versions = client.head(key());
      assertEquals(Set.of(deleted, live), Set.copyOf(versions));
      assertEquals(
          Status.Code.ABORTED,
          status(
              assertThrows(
                  Exception.class, () -> client.get(key(), "", root.resolve("mixed-conflict")))));
      assertEquals(
          Status.Code.NOT_FOUND,
          status(
              assertThrows(
                  Exception.class,
                  () ->
                      client.get(key(), deleted.getVersionId(), root.resolve("selected-delete")))));
      assertEquals(live, client.get(key(), live.getVersionId(), root.resolve("selected-live")));
      var merged =
          Wire.vector(deleted.getVector(), cluster.ring())
              .merge(Wire.vector(live.getVector(), cluster.ring()));
      var resolved = client.delete(key(), merged).getVersion();
      assertEquals(List.of(resolved), client.head(key()));
    }
  }

  @Test
  void readRepairCapturesMissingVersionsAndPreservesConcurrentSibling() throws Exception {
    var owners = cluster.owners(key());
    var vectorA = VectorClock.of(Map.of("node1", 50L));
    var vectorB = VectorClock.of(Map.of("node2", 50L));
    UUID live = UUID.randomUUID(), deleted = UUID.randomUUID();
    // Seed divergence directly, deliberately without hints; one live sibling and one tombstone.
    for (String node : owners.subList(0, 2)) {
      try (var upload =
          cluster
              .nodes
              .get(node)
              .store
              .beginReplica(
                  "demo",
                  key().getKey().toByteArray(),
                  0,
                  Wire.hash(new byte[0]).toByteArray(),
                  vectorA,
                  cluster.ring(),
                  live,
                  false)) {
        upload.commit();
      }
      try (var upload =
          cluster
              .nodes
              .get(node)
              .store
              .beginReplica(
                  "demo",
                  key().getKey().toByteArray(),
                  0,
                  Wire.hash(new byte[0]).toByteArray(),
                  vectorB,
                  cluster.ring(),
                  deleted,
                  true)) {
        upload.commit();
      }
    }
    cluster.disconnect(owners.get(2));
    String coordinator =
        cluster.ids.stream().filter(n -> !owners.contains(n)).findFirst().orElseThrow();
    try (var client = cluster.client(coordinator)) {
      assertEquals(2, client.head(key()).size());
    }
    eventually(() -> cluster.nodes.get(coordinator).store.hints().size() == 2);
    cluster.restart(coordinator); // Restart after durable capture, before target can accept repair.
    cluster.heal();
    eventually(
        () ->
            cluster
                    .nodes
                    .get(owners.get(2))
                    .store
                    .siblings("demo", key().getKey().toByteArray())
                    .size()
                == 2);
    var repaired =
        cluster.nodes.get(owners.get(2)).store.siblings("demo", key().getKey().toByteArray());
    assertEquals(Set.of(live, deleted), Set.copyOf(repaired.stream().map(e -> e.id()).toList()));
    assertEquals(1, repaired.stream().filter(e -> e.tombstone()).count());
    cluster.restart(owners.get(2));
    assertEquals(2, cluster.nodes.get(owners.get(2)).store.verifyAll());
  }

  @Test
  void exhaustedHintBudgetRejectsWriteBeforeOwnerPublication() throws Exception {
    var owners = cluster.owners(key());
    String offline = owners.get(2);
    cluster.disconnect(offline);
    var store = cluster.nodes.get("node4").store;
    synchronized (store) {
      for (int i = 0; i < ObjectStorage.MAX_HINTS; i++) {
        UUID id = UUID.randomUUID();
        var version =
            VersionMetadata.newBuilder()
                .setVersionId(id.toString())
                .setVector(Wire.vector(VectorClock.of(Map.of("node5", 1000L + i))))
                .setSha256(Wire.hash(new byte[0]))
                .setTombstone(true)
                .build();
        var header =
            ReplicaHeader.newBuilder()
                .setCluster(Wire.identity(cluster.ring()))
                .setObject(key())
                .setVersion(version)
                .setRequestId(id.toString())
                .build();
        store.enqueueHint(
            id,
            header.toByteArray(),
            List.of(offline),
            0,
            Wire.hash(new byte[0]).toByteArray(),
            new ByteArrayInputStream(new byte[0]));
      }
    }
    assertEquals(
        Status.Code.RESOURCE_EXHAUSTED,
        status(
            assertThrows(
                Exception.class,
                () -> put("node4", key(), payload("capacity", 1), VectorClock.empty()))));
    assertEquals(ObjectStorage.MAX_HINTS, store.hintStats().pendingVersions());
    for (String node : owners)
      assertTrue(
          cluster.nodes.get(node).store.siblings("demo", key().getKey().toByteArray()).isEmpty());
  }

  @Test
  void internalTombstoneReplayIsIdempotentAndRejectsLivePayload() throws Exception {
    String owner = cluster.owners(key()).getFirst();
    VersionMetadata version;
    try (var client = cluster.client("node4")) {
      version = client.delete(key(), VectorClock.empty()).getVersion();
    }
    var channel = cluster.direct(owner);
    try {
      Metadata token = new Metadata();
      token.put(PeerClient.TOKEN, cluster.access.peerToken());
      var stub =
          ReplicaStoreGrpc.newBlockingStub(
                  ClientInterceptors.intercept(
                      channel, MetadataUtils.newAttachHeadersInterceptor(token)))
              .withDeadlineAfter(5, TimeUnit.SECONDS);
      var header =
          ReplicaHeader.newBuilder()
              .setCluster(Wire.identity(cluster.ring()))
              .setObject(key())
              .setVersion(version)
              .setRequestId("tombstone-replay")
              .build();
      assertTrue(
          stub.applyTombstone(TombstoneRequest.newBuilder().setHeader(header).build())
              .getDurable());
      long sequence =
          cluster
              .nodes
              .get(owner)
              .store
              .head("demo", key().getKey().toByteArray(), Wire.uuid(version.getVersionId()))
              .sequence();
      assertTrue(
          stub.applyTombstone(TombstoneRequest.newBuilder().setHeader(header).build())
              .getDurable());
      assertEquals(
          sequence,
          cluster
              .nodes
              .get(owner)
              .store
              .head("demo", key().getKey().toByteArray(), Wire.uuid(version.getVersionId()))
              .sequence());
      assertEquals(
          Status.Code.INVALID_ARGUMENT,
          status(
              assertThrows(
                  Exception.class,
                  () ->
                      stub.applyTombstone(
                          TombstoneRequest.newBuilder()
                              .setHeader(
                                  header.toBuilder()
                                      .setVersion(version.toBuilder().setTombstone(false)))
                              .build()))));
    } finally {
      channel.shutdownNow();
    }
  }

  private final class Cluster implements AutoCloseable {
    final List<String> ids = List.of("node1", "node2", "node3", "node4", "node5");
    final Map<String, Node> nodes = new LinkedHashMap<>();
    final Map<String, Map<String, Link>> links = new HashMap<>();
    final Access access =
        new Access(UUID.randomUUID().toString(), UUID.randomUUID().toString(), Set.of("demo"));

    Cluster() throws Exception {
      cluster = this;
      for (String id : ids) nodes.put(id, new Node(id));
      for (String source : ids) {
        Map<String, Link> outgoing = new HashMap<>();
        for (String target : ids) outgoing.put(target, new Link(() -> nodes.get(target).port));
        links.put(source, outgoing);
      }
      for (Node node : nodes.values()) node.start();
    }

    io.quorumfs.ring.HashRing ring() {
      return nodes.get("node1").config.ring();
    }

    List<String> owners(ObjectKey key) {
      return ring().owners(key.getNamespace(), key.getKey().toByteArray());
    }

    ManagedChannel direct(String node) {
      return NettyChannelBuilder.forAddress("localhost", nodes.get(node).port)
          .usePlaintext()
          .maxInboundMessageSize(2 * 1024 * 1024)
          .build();
    }

    ObjectClient client(String node) {
      return client(node, access.clientToken(), 8000);
    }

    ObjectClient client(String node, String token, long millis) {
      return new ObjectClient(direct(node), token, Duration.ofMillis(millis));
    }

    void partition(Set<String> group) {
      links.forEach(
          (source, outgoing) ->
              outgoing.forEach(
                  (target, link) ->
                      link.enabled(group.contains(source) == group.contains(target))));
    }

    void disconnect(String target) {
      links.values().forEach(outgoing -> outgoing.get(target).enabled(false));
    }

    void heal() {
      links.values().forEach(outgoing -> outgoing.values().forEach(link -> link.enabled(true)));
    }

    void restart(String node) throws Exception {
      nodes.get(node).close();
      nodes.get(node).start();
    }

    @Override
    public void close() throws IOException {
      for (Node node : nodes.values()) node.close();
      for (var outgoing : links.values()) for (Link link : outgoing.values()) link.close();
    }
  }

  private final class Node implements AutoCloseable {
    final NodeConfig config;
    volatile int port;
    volatile String fault = "";
    ObjectStorage store;
    Server server;
    CoordinatorService coordinator;
    ExecutorService executor;

    Node(String id) {
      config =
          new NodeConfig(
              "integration",
              1,
              id,
              9000,
              root.resolve(id),
              java.util.stream.IntStream.rangeClosed(1, 5)
                  .mapToObj(i -> new NodeConfig.Member("node" + i, "localhost:" + (9000 + i)))
                  .toList(),
              3,
              2,
              2);
    }

    void start() throws Exception {
      store = new ObjectStorage(config.dataDir(), config.identity());
      store.configureRing(config.ring(), config.nodeId());
      Path transfers = Spool.prepare(config.dataDir().resolve("transfers"));
      Map<String, ManagedChannel> channels = new HashMap<>();
      cluster
          .links
          .get(config.nodeId())
          .forEach(
              (node, link) ->
                  channels.put(
                      node,
                      NettyChannelBuilder.forAddress("localhost", link.port())
                          .usePlaintext()
                          .maxInboundMessageSize(2 * 1024 * 1024)
                          .build()));
      coordinator =
          new CoordinatorService(
              store,
              config.ring(),
              new PeerClient(channels, config.ring(), cluster.access.peerToken()),
              ServerMain.clusterInfo(config, true),
              transfers,
              Set.of("demo"));
      executor = Executors.newVirtualThreadPerTaskExecutor();
      server =
          NettyServerBuilder.forPort(0)
              .executor(executor)
              .maxInboundMessageSize(300 * 1024)
              .addService(
                  ServerInterceptors.intercept(coordinator, cluster.access.interceptor(false)))
              .addService(
                  ServerInterceptors.intercept(
                      new ReplicaService(store, config.ring(), config.nodeId(), transfers),
                      faults(),
                      cluster.access.interceptor(true)))
              .build()
              .start();
      port = server.getPort();
    }

    private ServerInterceptor faults() {
      return new ServerInterceptor() {
        @Override
        public <Q, A> ServerCall.Listener<Q> interceptCall(
            ServerCall<Q, A> call, Metadata headers, ServerCallHandler<Q, A> next) {
          return next.startCall(
              new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                @Override
                public void sendMessage(A message) {
                  if (message instanceof ReplicaAck ack) {
                    String selected = fault;
                    if (selected.equals("slow-ack")) {
                      try {
                        Thread.sleep(4000);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    }
                    var builder = ack.toBuilder();
                    switch (selected) {
                      case "wrong-node" -> builder.setNodeId("impostor");
                      case "not-durable" -> builder.setDurable(false);
                      case "wrong-version" -> builder.setVersionId(UUID.randomUUID().toString());
                      case "wrong-epoch" ->
                          builder.setCluster(ack.getCluster().toBuilder().setEpoch(2));
                      default -> {}
                    }
                    @SuppressWarnings("unchecked")
                    A changed = (A) builder.build();
                    super.sendMessage(changed);
                  } else if (message instanceof ReplicaVersions versions
                      && fault.equals("bad-read")) {
                    @SuppressWarnings("unchecked")
                    A invalid = (A) versions.toBuilder().setNodeId("impostor").build();
                    super.sendMessage(invalid);
                  } else if (message instanceof ReplicaFrame frame
                      && frame.hasChunk()
                      && fault.equals("bad-fetch")) {
                    var changed =
                        frame.toBuilder()
                            .setChunk(
                                frame.getChunk().toBuilder().setSha256(Wire.hash(new byte[] {9})))
                            .build();
                    @SuppressWarnings("unchecked")
                    A damaged = (A) changed;
                    super.sendMessage(damaged);
                  } else super.sendMessage(message);
                }
              },
              headers);
        }
      };
    }

    @Override
    public void close() throws IOException {
      if (server == null) return;
      server.shutdownNow();
      try {
        server.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      coordinator.close();
      executor.shutdownNow();
      store.close();
      port = 0;
      server = null;
    }
  }

  /** Actual TCP connections per directed node pair; cutting a link closes established streams. */
  private static final class Link implements AutoCloseable {
    private final ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private volatile boolean enabled = true;

    Link(IntSupplier target) throws IOException {
      workers.submit(
          () -> {
            while (!listener.isClosed()) {
              try {
                Socket source = listener.accept();
                sockets.add(source);
                if (!enabled) {
                  source.close();
                  sockets.remove(source);
                  continue;
                }
                Socket destination = new Socket("localhost", target.getAsInt());
                sockets.add(destination);
                workers.submit(() -> copy(source, destination));
                workers.submit(() -> copy(destination, source));
              } catch (IOException e) {
                if (listener.isClosed()) return;
              }
            }
          });
    }

    int port() {
      return listener.getLocalPort();
    }

    void enabled(boolean value) {
      enabled = value;
      if (!value) sockets.forEach(Link::quietClose);
    }

    private void copy(Socket source, Socket destination) {
      try {
        source.getInputStream().transferTo(destination.getOutputStream());
      } catch (IOException ignored) {
      } finally {
        quietClose(source);
        quietClose(destination);
        sockets.remove(source);
        sockets.remove(destination);
      }
    }

    private static void quietClose(Socket socket) {
      try {
        socket.close();
      } catch (IOException ignored) {
      }
    }

    @Override
    public void close() throws IOException {
      listener.close();
      sockets.forEach(Link::quietClose);
      workers.shutdownNow();
    }
  }
}
