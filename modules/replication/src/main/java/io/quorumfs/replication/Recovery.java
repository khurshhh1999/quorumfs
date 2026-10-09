package io.quorumfs.replication;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.storage.ObjectStorage;
import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/** One bounded delivery worker per node; durable intents and payloads live in the storage WAL. */
public final class Recovery implements AutoCloseable {
  private final ObjectStorage store;
  private final PeerClient peers;
  private final HashRing ring;
  private final Path transfers;
  private final ScheduledExecutorService delivery = Executors.newSingleThreadScheduledExecutor();
  private final ExecutorService preparation = Executors.newVirtualThreadPerTaskExecutor();
  private final Semaphore repairs = new Semaphore(2);
  private final Map<String, Retry> retries = new HashMap<>();
  private String lastHint = "";

  private record Retry(int failures, long next) {}

  public Recovery(ObjectStorage store, PeerClient peers, HashRing ring, Path transfers) {
    this.store = store;
    this.peers = peers;
    this.ring = ring;
    this.transfers = transfers;
    delivery.scheduleWithFixedDelay(Context.ROOT.wrap(this::tick), 1, 1, TimeUnit.SECONDS);
  }

  public void persist(ReplicaHeader header, Spool spool, List<String> destinations)
      throws IOException {
    var stable = header.toBuilder().setRequestId(header.getVersion().getVersionId()).build();
    try (var input = spool.input()) {
      store.enqueueHint(
          Wire.uuid(header.getVersion().getVersionId()),
          stable.toByteArray(),
          destinations,
          header.getVersion().getSize(),
          header.getVersion().getSha256().toByteArray(),
          input);
    }
  }

  /** Discovery is best effort; once captured, repair uses the same durable queue as writes. */
  public void repair(
      ObjectKey key,
      List<VersionMetadata> versions,
      Map<String, List<String>> holders,
      Map<String, List<VersionMetadata>> observed) {
    if (!repairs.tryAcquire()) {
      System.out.println("event=read_repair_deferred reason=admission");
      return;
    }
    try {
      preparation.execute(
          Context.ROOT.wrap(
              () -> {
                try {
                  for (var version : versions) {
                    List<String> targets =
                        ring.owners(key.getNamespace(), key.getKey().toByteArray()).stream()
                            .filter(
                                node ->
                                    observed.getOrDefault(node, List.of()).stream()
                                        .noneMatch(
                                            v ->
                                                v.equals(version)
                                                    || Wire.vector(version.getVector(), ring)
                                                            .compare(
                                                                Wire.vector(v.getVector(), ring))
                                                        == io.quorumfs.versioning.VectorClock
                                                            .Relation.BEFORE))
                            .toList();
                    if (targets.isEmpty()) continue;
                    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    for (String holder : holders.get(version.getVersionId())) {
                      try (var data = peers.fetch(holder, key, version, transfers, end)) {
                        persist(
                            ReplicaHeader.newBuilder()
                                .setCluster(Wire.identity(ring))
                                .setObject(key)
                                .setVersion(version)
                                .build(),
                            data,
                            targets);
                        break;
                      } catch (Exception e) {
                        if (Thread.currentThread().isInterrupted()) return;
                        System.out.println("event=read_repair_deferred reason=capture");
                      }
                    }
                  }
                } finally {
                  repairs.release();
                }
              }));
    } catch (RejectedExecutionException e) {
      repairs.release();
    }
  }

  private void tick() {
    try {
      var pending =
          store.hints().stream().sorted(Comparator.comparing(h -> h.id().toString())).toList();
      Set<String> active = new HashSet<>();
      for (var hint : pending)
        for (String target : hint.destinations()) active.add(hint.id() + "/" + target);
      retries.keySet().retainAll(active);
      // Round robin prevents permanently unreachable early entries from starving later work.
      int start = 0;
      while (start < pending.size() && pending.get(start).id().toString().compareTo(lastHint) <= 0)
        start++;
      for (int step = 0; step < pending.size(); step++) {
        var hint = pending.get((start + step) % pending.size());
        var due =
            hint.destinations().stream()
                .filter(
                    node ->
                        retries.getOrDefault(hint.id() + "/" + node, new Retry(0, 0)).next()
                            <= System.nanoTime())
                .toList();
        if (due.isEmpty()) continue;
        lastHint = hint.id().toString();
        var header = ReplicaHeader.parseFrom(hint.metadata());
        Wire.compatible(header.getCluster(), ring);
        Wire.key(header.getObject());
        Wire.version(header.getVersion(), ring);
        Wire.request(header.getRequestId());
        if (!hint.id().toString().equals(header.getVersion().getVersionId())
            || hint.size() != header.getVersion().getSize()
            || !Arrays.equals(hint.sha256(), header.getVersion().getSha256().toByteArray())
            || !ring.owners(
                    header.getObject().getNamespace(), header.getObject().getKey().toByteArray())
                .containsAll(hint.destinations()))
          throw new IOException("Invalid persisted delivery identity");
        try (Spool data = new Spool(transfers, hint.size(), hint.sha256())) {
          store.readHint(
              hint.id(),
              new OutputStream() {
                private long offset;

                @Override
                public void write(int b) {
                  throw new UnsupportedOperationException();
                }

                @Override
                public void write(byte[] bytes, int start, int length) throws IOException {
                  byte[] part = Arrays.copyOfRange(bytes, start, start + length);
                  data.append(
                      ObjectChunk.newBuilder()
                          .setOffset(offset)
                          .setData(ByteString.copyFrom(part))
                          .setSha256(Wire.hash(part))
                          .build());
                  offset += length;
                }
              });
          data.finish();
          for (String node : due) {
            if (Thread.currentThread().isInterrupted()) return;
            String key = hint.id() + "/" + node;
            try {
              peers.deliver(node, header, data, System.nanoTime() + TimeUnit.SECONDS.toNanos(4));
              store.acknowledgeHint(hint.id(), node);
              // Brief cooldown prevents a racing read from continuously re-enqueuing the same work.
              retries.put(key, new Retry(0, System.nanoTime() + TimeUnit.SECONDS.toNanos(1)));
              System.out.printf("event=hint_ack destination=%s%n", node);
            } catch (Exception e) {
              int failures = Math.min(6, retries.getOrDefault(key, new Retry(0, 0)).failures() + 1);
              retries.put(
                  key,
                  new Retry(
                      failures, System.nanoTime() + TimeUnit.SECONDS.toNanos(1L << failures)));
              System.out.printf("event=hint_retry destination=%s attempt=%d%n", node, failures);
            }
          }
        }
        break;
      }
      var stats = store.hintStats();
      if (stats.pendingVersions() > 0)
        System.out.printf(
            "event=hint_backlog versions=%d deliveries=%d bytes=%d oldest_age_ms=%d%n",
            stats.pendingVersions(),
            stats.pendingDeliveries(),
            stats.bytes(),
            Math.max(0, System.currentTimeMillis() - stats.oldestMillis()));
    } catch (Exception e) {
      if (!Thread.currentThread().isInterrupted()) System.out.println("event=hint_worker_error");
    }
  }

  @Override
  public void close() {
    preparation.shutdownNow();
    delivery.shutdownNow();
    preparation.close();
    delivery.close();
  }
}
