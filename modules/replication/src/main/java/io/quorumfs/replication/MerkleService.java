package io.quorumfs.replication;

import com.google.protobuf.ByteString;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.ring.HashRing;
import io.quorumfs.storage.ObjectStorage;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Detached snapshots expire after sixty seconds; at most four are retained per node. */
final class MerkleService {
  private final ObjectStorage store;
  private final HashRing ring;
  private final String node;
  private final java.util.function.LongSupplier clock;
  private final Map<String, View> views = new HashMap<>();

  private record View(String peer, long created, List<RangeVersion> entries) {}

  MerkleService(ObjectStorage store, HashRing ring, String node) {
    this(store, ring, node, System::nanoTime);
  }

  MerkleService(
      ObjectStorage store, HashRing ring, String node, java.util.function.LongSupplier clock) {
    this.clock = clock;
    this.store = store;
    this.ring = ring;
    this.node = node;
  }

  private void check(ClusterIdentity identity, String peer) {
    Wire.compatible(identity, ring);
    if (!ring.nodeIds().contains(peer) || node.equals(peer))
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    long now = clock.getAsLong();
    views.values().removeIf(v -> now - v.created() >= TimeUnit.SECONDS.toNanos(60));
  }

  private View view(String id, String peer) {
    Wire.uuid(id);
    View view = views.get(id);
    if (view == null || !view.peer().equals(peer))
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    return view;
  }

  synchronized MerkleResponse exchange(MerkleRequest request) throws Exception {
    check(request.getCluster(), request.getPeerNodeId());
    Merkle.index(request.getRange(), 0);
    if (request.getDepth() < 0 || request.getDepth() > Merkle.DEPTH)
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    String id = request.getSnapshotId();
    if (id.isEmpty()) {
      // A peer replaces its previous view, bounding both stale views and admission fairly.
      views.values().removeIf(v -> v.peer().equals(request.getPeerNodeId()));
      if (views.size() >= 4) throw Errors.exception(ErrorReason.CAPACITY_EXHAUSTED);
      id = UUID.randomUUID().toString();
      var entries = Merkle.entries(store.inventory(request.getPeerNodeId()));
      views.put(id, new View(request.getPeerNodeId(), clock.getAsLong(), entries));
    }
    View view = view(id, request.getPeerNodeId());
    return MerkleResponse.newBuilder()
        .setCluster(Wire.identity(ring))
        .setNodeId(node)
        .setSnapshotId(id)
        .addAllNodes(Merkle.tree(view.entries(), request.getDepth()))
        .build();
  }

  synchronized List<RangeVersion> page(RangeRequest request) {
    check(request.getCluster(), request.getPeerNodeId());
    int leaf = Merkle.index(request.getRange(), Merkle.DEPTH);
    if (request.getLimit() < 1 || request.getLimit() > 128)
      throw Errors.exception(ErrorReason.INVALID_REQUEST);
    View view = view(request.getSnapshotId(), request.getPeerNodeId());
    var entries = view.entries().stream().filter(e -> Merkle.leaf(e.getObject()) == leaf).toList();
    int offset = 0;
    UUID id = Wire.uuid(request.getSnapshotId());
    if (!request.getResumeToken().isEmpty()) {
      if (request.getResumeToken().size() != 24)
        throw Errors.exception(ErrorReason.INVALID_REQUEST);
      var cursor = request.getResumeToken().asReadOnlyByteBuffer();
      if (cursor.getLong() != id.getMostSignificantBits()
          || cursor.getLong() != id.getLeastSignificantBits()
          || cursor.getInt() != leaf) throw Errors.exception(ErrorReason.INVALID_REQUEST);
      offset = cursor.getInt();
      if (offset < 1 || offset > entries.size())
        throw Errors.exception(ErrorReason.INVALID_REQUEST);
    }
    List<RangeVersion> page = new ArrayList<>();
    for (int i = offset; i < Math.min(entries.size(), offset + request.getLimit()); i++) {
      byte[] cursor =
          ByteBuffer.allocate(24)
              .putLong(id.getMostSignificantBits())
              .putLong(id.getLeastSignificantBits())
              .putInt(leaf)
              .putInt(i + 1)
              .array();
      page.add(entries.get(i).toBuilder().setResumeToken(ByteString.copyFrom(cursor)).build());
    }
    return List.copyOf(page);
  }
}
