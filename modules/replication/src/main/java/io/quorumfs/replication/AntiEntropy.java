package io.quorumfs.replication;

import io.grpc.Context;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.storage.ObjectStorage;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Restartable snapshot comparisons; replica publication is the durable progress record. */
public final class AntiEntropy implements AutoCloseable {
  private final ObjectStorage store;
  private final HashRing ring;
  private final String node;
  private final PeerClient peers;
  private final Path transfers;
  private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor();
  private final List<String> targets;
  private int next;
  private final Map<String, Integer> leafCursors = new HashMap<>();
  private final Map<String, String> entryCursors = new HashMap<>();
  private boolean started;

  public AntiEntropy(
      ObjectStorage store, HashRing ring, String node, PeerClient peers, Path transfers) {
    this.store = store;
    this.ring = ring;
    this.node = node;
    this.peers = peers;
    this.transfers = transfers;
    targets = ring.nodeIds().stream().filter(n -> !n.equals(node)).sorted().toList();
  }

  public synchronized void start(Duration interval) {
    if (started || interval.toMillis() < 1000 || interval.toMillis() > 300000)
      throw new IllegalArgumentException(
          "Anti-entropy interval must be 1000..300000 milliseconds; start once");
    started = true;
    worker.scheduleWithFixedDelay(
        Context.ROOT.wrap(
            () -> {
              String peer = targets.get(next);
              next = (next + 1) % targets.size();
              try {
                round(peer);
              } catch (Exception e) {
                if (!Thread.currentThread().isInterrupted())
                  System.out.printf(
                      "event=anti_entropy_retry peer=%s status=%s%n",
                      peer, io.grpc.Status.fromThrowable(RpcFailure.map(e)).getCode());
              }
            }),
        interval.toMillis(),
        interval.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  public synchronized void round(String peer) throws Exception {
    if (!targets.contains(peer)) throw Errors.exception(ErrorReason.INVALID_REQUEST);
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
    var local = Merkle.entries(store.inventory(peer));
    var remote = peers.merkle(peer, node, "", 0, end);
    String snapshot = remote.getSnapshotId();
    if (Merkle.tree(local, 0).equals(remote.getNodesList())) {
      System.out.printf(
          "event=anti_entropy_equal peer=%s corrupt_versions=%d%n",
          peer, local.stream().filter(RangeVersion::getCorrupt).count());
      return;
    }
    // Descend fixed levels; ranges are identical on every pair of canonical owners.
    Set<Integer> divergent = Set.of(0);
    for (int depth = 1; depth <= Merkle.DEPTH; depth++) {
      remote = peers.merkle(peer, node, snapshot, depth, end);
      var localNodes = Merkle.tree(local, depth);
      Set<Integer> children = new TreeSet<>();
      for (int i = 0; i < localNodes.size(); i++)
        if (divergent.contains(i / 2) && !localNodes.get(i).equals(remote.getNodes(i)))
          children.add(i);
      divergent = children;
    }
    long bytes = 0;
    int attempts = 0, repaired = 0, failures = 0;
    int startLeaf = leafCursors.getOrDefault(peer, 0);
    List<Integer> orderedLeaves = new ArrayList<>(divergent);
    orderedLeaves.sort(Comparator.comparingInt(leaf -> (leaf - startLeaf + 16) % 16));
    for (int leaf : orderedLeaves) {
      leafCursors.put(peer, (leaf + 1) % 16);
      String cursorKey = peer + "/" + leaf;
      String resume = entryCursors.getOrDefault(cursorKey, "");
      var remoteEntries = peers.range(peer, node, snapshot, leaf, end);
      if (!Merkle.tree(remoteEntries, Merkle.DEPTH).get(leaf).equals(remote.getNodes(leaf)))
        throw Errors.exception(ErrorReason.CHECKSUM_MISMATCH);
      var localEntries = local.stream().filter(e -> Merkle.leaf(e.getObject()) == leaf).toList();
      Map<ObjectKey, List<VersionMetadata>> candidates =
          new TreeMap<>(
              Comparator.comparing(
                  k -> Merkle.identity(RangeVersion.newBuilder().setObject(k).build())));
      Map<String, RangeVersion> here = new HashMap<>(), there = new HashMap<>();
      for (var entry : localEntries) {
        here.put(Merkle.identity(entry), entry);
        candidates
            .computeIfAbsent(entry.getObject(), k -> new ArrayList<>())
            .add(entry.getVersion());
      }
      for (var entry : remoteEntries) {
        there.put(Merkle.identity(entry), entry);
        candidates
            .computeIfAbsent(entry.getObject(), k -> new ArrayList<>())
            .add(entry.getVersion());
      }
      for (var object : candidates.entrySet()) {
        for (var version : Wire.merge(object.getValue(), ring)) {
          String key =
              Merkle.identity(
                  RangeVersion.newBuilder().setObject(object.getKey()).setVersion(version).build());
          var a = here.get(key);
          var b = there.get(key);
          boolean healthyA = a != null && !a.getCorrupt(), healthyB = b != null && !b.getCorrupt();
          if (healthyA == healthyB || key.compareTo(resume) <= 0) continue;
          if (attempts == 128 || version.getSize() > 128L * 1024 * 1024 - bytes) {
            System.out.printf(
                "event=anti_entropy_budget peer=%s attempts=%d bytes=%d%n", peer, attempts, bytes);
            return;
          }
          entryCursors.put(cursorKey, key);
          attempts++;
          bytes += version.getSize();
          String source = healthyA ? node : peer, destination = healthyA ? peer : node;
          try (var payload = peers.fetch(source, object.getKey(), version, transfers, end)) {
            var header =
                ReplicaHeader.newBuilder()
                    .setCluster(Wire.identity(ring))
                    .setObject(object.getKey())
                    .setVersion(version)
                    .setRequestId(version.getVersionId())
                    .build();
            peers.deliver(destination, header, payload, end);
            repaired++;
          } catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) throw e;
            failures++;
          }
        }
      }
      entryCursors.remove(cursorKey);
    }
    System.out.printf(
        "event=anti_entropy_round peer=%s divergent_leaves=%d repaired=%d failures=%d bytes=%d%n",
        peer, divergent.size(), repaired, failures, bytes);
  }

  @Override
  public void close() {
    worker.shutdownNow();
    worker.close();
  }
}
