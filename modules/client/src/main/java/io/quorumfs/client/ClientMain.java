package io.quorumfs.client;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.quorumfs.protocol.v1.*;
import java.util.concurrent.TimeUnit;

/** Development CLI; object commands require an explicit bearer credential. */
public final class ClientMain {
  private ClientMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length >= 3
        && java.util.Set.of("put", "get", "head", "resolve", "owners", "context")
            .contains(args[0])) {
      object(args);
      return;
    }
    if (args.length != 3 || !args[2].equals("--insecure")) {
      throw new IllegalArgumentException(
          "Usage: client <health|info|assert-unimplemented> <host:port> --insecure");
    }
    ManagedChannel channel = NettyChannelBuilder.forTarget(args[1]).usePlaintext().build();
    try {
      switch (args[0]) {
        case "health" -> {
          var reply =
              HealthGrpc.newBlockingStub(channel)
                  .withDeadlineAfter(2, TimeUnit.SECONDS)
                  .check(HealthCheckRequest.getDefaultInstance());
          if (reply.getStatus() != HealthCheckResponse.ServingStatus.SERVING)
            throw new IllegalStateException("Node not serving");
          System.out.println("SERVING");
        }
        case "info" -> {
          var info =
              ObjectStoreGrpc.newBlockingStub(channel)
                  .withDeadlineAfter(2, TimeUnit.SECONDS)
                  .getClusterInfo(GetClusterInfoRequest.getDefaultInstance());
          System.out.printf(
              "%s %s %d %d %d %d %d %s%n",
              info.getNodeId(),
              info.getIdentity().getClusterId(),
              info.getIdentity().getEpoch(),
              info.getMembersCount(),
              info.getReplicationFactor(),
              info.getReadQuorum(),
              info.getWriteQuorum(),
              info.getObjectApiReady());
        }
        case "assert-unimplemented" -> {
          try {
            ObjectStoreGrpc.newBlockingStub(channel)
                .withDeadlineAfter(2, TimeUnit.SECONDS)
                .headObject(HeadObjectRequest.getDefaultInstance());
            throw new IllegalStateException("Unexpected successful object API");
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.UNIMPLEMENTED) throw e;
            System.out.println("UNIMPLEMENTED");
          }
        }
        default -> throw new IllegalArgumentException("Unknown command");
      }
    } finally {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void object(String[] args) throws Exception {
    if (!args[2].equals("--insecure") || args.length < 5)
      throw new IllegalArgumentException(
          "Usage: client <put|resolve|get|head|owners|context> endpoint --insecure namespace key [file] [context-hex|version-id]");
    var key = ObjectClient.key(args[3], args[4]);
    int count = args.length;
    boolean valid =
        switch (args[0]) {
          case "head", "owners", "context" -> count == 5;
          case "put", "get" -> count == 6 || count == 7;
          case "resolve" -> count == 7;
          default -> false;
        };
    if (!valid) throw new IllegalArgumentException("Invalid object command arguments");
    var channel =
        NettyChannelBuilder.forTarget(args[1])
            .usePlaintext()
            .maxInboundMessageSize(2 * 1024 * 1024)
            .build();
    long timeout = Long.parseLong(System.getenv().getOrDefault("QUORUMFS_TIMEOUT_MS", "10000"));
    try (var client =
        new ObjectClient(
            channel,
            System.getenv().getOrDefault("QUORUMFS_CLIENT_TOKEN", ""),
            java.time.Duration.ofMillis(timeout))) {
      switch (args[0]) {
        case "owners" -> {
          var info =
              ObjectStoreGrpc.newBlockingStub(channel)
                  .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
                  .getClusterInfo(GetClusterInfoRequest.getDefaultInstance());
          var ring =
              new io.quorumfs.ring.HashRing(
                  info.getIdentity().getClusterId(),
                  info.getIdentity().getEpoch(),
                  info.getMembersList().stream()
                      .map(
                          m -> new io.quorumfs.ring.HashRing.Member(m.getNodeId(), m.getEndpoint()))
                      .toList(),
                  info.getReplicationFactor(),
                  info.getReadQuorum(),
                  info.getWriteQuorum());
          System.out.println(
              String.join(",", ring.owners(key.getNamespace(), key.getKey().toByteArray())));
        }
        case "context" -> {
          var context = io.quorumfs.versioning.VectorClock.empty();
          for (var version : client.head(key))
            context =
                context.merge(
                    io.quorumfs.versioning.VectorClock.of(version.getVector().getCountersMap()));
          System.out.println(java.util.HexFormat.of().formatHex(context.serialize()));
        }
        case "head" -> {
          for (var version : client.head(key)) print(version);
        }
        case "get" ->
            print(client.get(key, count == 7 ? args[6] : "", java.nio.file.Path.of(args[5])));
        case "put", "resolve" -> {
          var context =
              count == 6 || args[6].equals("-")
                  ? io.quorumfs.versioning.VectorClock.empty()
                  : io.quorumfs.versioning.VectorClock.deserialize(
                      java.util.HexFormat.of().parseHex(args[6]));
          var result =
              client.put(key, java.nio.file.Path.of(args[5]), context, args[0].equals("resolve"));
          print(result.getVersion());
          System.out.println("durable_owners=" + result.getDurableOwnerCount());
        }
        default -> throw new AssertionError();
      }
    }
  }

  private static void print(VersionMetadata version) {
    System.out.printf(
        "%s %d %s %s%n",
        version.getVersionId(),
        version.getSize(),
        java.util.HexFormat.of().formatHex(version.getSha256().toByteArray()),
        java.util.HexFormat.of()
            .formatHex(
                io.quorumfs.versioning.VectorClock.of(version.getVector().getCountersMap())
                    .serialize()));
  }
}
