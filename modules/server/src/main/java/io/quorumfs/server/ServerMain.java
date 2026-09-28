package io.quorumfs.server;

import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.storage.IdentityStore;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public final class ServerMain {
  private ServerMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("Usage: server <node.properties>");
    NodeConfig config = NodeConfig.load(Path.of(args[0]));
    IdentityStore identity = new IdentityStore(config.dataDir(), config.identity());
    HealthStatusManager health = new HealthStatusManager();
    health.setStatus("", ServingStatus.NOT_SERVING);
    health.setStatus(ObjectStoreGrpc.SERVICE_NAME, ServingStatus.NOT_SERVING);
    Server server =
        NettyServerBuilder.forPort(config.port())
            .maxInboundMessageSize(300 * 1024)
            .maxConcurrentCallsPerConnection(32)
            .addService(health.getHealthService())
            .addService(new ClusterService(config))
            .addService(new ReplicaStoreGrpc.ReplicaStoreImplBase() {})
            .build();
    try {
      server.start();
    } catch (Exception e) {
      identity.close();
      throw e;
    }
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  health.enterTerminalState();
                  server.shutdown();
                  try {
                    if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                      server.shutdownNow();
                      server.awaitTermination(5, TimeUnit.SECONDS);
                    }
                  } catch (InterruptedException e) {
                    server.shutdownNow();
                    Thread.currentThread().interrupt();
                  } finally {
                    identity.close();
                  }
                },
                "quorumfs-shutdown"));
    health.setStatus("", ServingStatus.SERVING);
    System.out.printf(
        "event=node_started node=%s cluster=%s epoch=%d port=%d object_api_ready=false%n",
        config.nodeId(), config.clusterId(), config.epoch(), config.port());
    server.awaitTermination();
  }

  static final class ClusterService extends ObjectStoreGrpc.ObjectStoreImplBase {
    private final NodeConfig config;

    ClusterService(NodeConfig config) {
      this.config = config;
    }

    @Override
    public void getClusterInfo(
        GetClusterInfoRequest request, StreamObserver<ClusterInfo> observer) {
      ClusterInfo.Builder info =
          ClusterInfo.newBuilder()
              .setIdentity(
                  ClusterIdentity.newBuilder()
                      .setClusterId(config.clusterId())
                      .setEpoch(config.epoch()))
              .setNodeId(config.nodeId())
              .setReplicationFactor(config.replicas())
              .setReadQuorum(config.reads())
              .setWriteQuorum(config.writes())
              .setApiVersion("quorumfs.v1")
              .setObjectApiReady(false);
      config
          .members()
          .forEach(
              m -> info.addMembers(Node.newBuilder().setNodeId(m.id()).setEndpoint(m.endpoint())));
      observer.onNext(info.build());
      observer.onCompleted();
    }
  }
}
