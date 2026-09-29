package io.quorumfs.client;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.quorumfs.protocol.v1.*;
import java.util.concurrent.TimeUnit;

/** Development-only inspection CLI; no object data commands are advertised. */
public final class ClientMain {
  private ClientMain() {}

  public static void main(String[] args) throws Exception {
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
}
