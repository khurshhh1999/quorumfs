package io.quorumfs.server;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import io.quorumfs.coordinator.CoordinatorService;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.replication.*;
import io.quorumfs.storage.ObjectStorage;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class ServerMain {
  private ServerMain() {}

  public static void main(String[] args) throws Exception {
    if (args.length > 0 && args[0].equals("storage")) {
      StorageMain.main(java.util.Arrays.copyOfRange(args, 1, args.length));
      return;
    }
    if (args.length != 1) throw new IllegalArgumentException("Usage: server <node.properties>");
    Access access = Access.environment();
    NodeConfig config = NodeConfig.load(Path.of(args[0]));
    ObjectStorage storage = new ObjectStorage(config.dataDir(), config.identity());
    try {
      storage.configureRing(config.ring(), config.nodeId());
    } catch (Exception e) {
      storage.close();
      throw e;
    }
    System.out.printf(
        "event=ring_loaded node=%s epoch=%d fingerprint=%s vnodes=128%n",
        config.nodeId(), config.epoch(), config.ring().fingerprint());
    System.out.printf(
        "event=storage_open node=%s recovered_uploads=%d%n",
        config.nodeId(), storage.stats().recovered());
    HealthStatusManager health = new HealthStatusManager();
    health.setStatus("", ServingStatus.NOT_SERVING);
    health.setStatus(ObjectStoreGrpc.SERVICE_NAME, ServingStatus.NOT_SERVING);
    CoordinatorService coordinator = null;
    var executor = Executors.newVirtualThreadPerTaskExecutor();
    var builder =
        NettyServerBuilder.forPort(config.port())
            .executor(executor)
            .maxInboundMessageSize(300 * 1024)
            .maxConcurrentCallsPerConnection(32)
            .addService(health.getHealthService());
    if (access == null) {
      builder.addService(new ClusterService(config));
      builder.addService(new ReplicaStoreGrpc.ReplicaStoreImplBase() {});
    } else {
      var transfers = Spool.prepare(config.dataDir().resolve("transfers"));
      Map<String, ManagedChannel> channels = new HashMap<>();
      for (var member : config.members())
        channels.put(
            member.id(),
            NettyChannelBuilder.forTarget(member.endpoint())
                .usePlaintext()
                .maxInboundMessageSize(2 * 1024 * 1024)
                .build());
      var peers = new PeerClient(channels, config.ring(), access.peerToken());
      coordinator =
          new CoordinatorService(
              storage,
              config.ring(),
              peers,
              clusterInfo(config, true),
              transfers,
              access.namespaces());
      builder.addService(ServerInterceptors.intercept(coordinator, access.interceptor(false)));
      builder.addService(
          ServerInterceptors.intercept(
              new ReplicaService(storage, config.ring(), config.nodeId(), transfers),
              access.interceptor(true)));
    }
    CoordinatorService running = coordinator;
    Server server = builder.build();
    try {
      server.start();
    } catch (Exception e) {
      if (running != null) running.close();
      executor.shutdownNow();
      storage.close();
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
                    if (running != null) running.close();
                    executor.shutdownNow();
                    storage.close();
                  }
                },
                "quorumfs-shutdown"));
    health.setStatus("", ServingStatus.SERVING);
    if (access != null) health.setStatus(ObjectStoreGrpc.SERVICE_NAME, ServingStatus.SERVING);
    System.out.printf(
        "event=node_started node=%s cluster=%s epoch=%d port=%d object_api_ready=%s%n",
        config.nodeId(), config.clusterId(), config.epoch(), config.port(), access != null);
    server.awaitTermination();
  }

  static ClusterInfo clusterInfo(NodeConfig config, boolean ready) {
    ClusterInfo.Builder info =
        ClusterInfo.newBuilder()
            .setIdentity(io.quorumfs.protocol.Wire.identity(config.ring()))
            .setNodeId(config.nodeId())
            .setReplicationFactor(config.replicas())
            .setReadQuorum(config.reads())
            .setWriteQuorum(config.writes())
            .setApiVersion("quorumfs.v1")
            .setObjectApiReady(ready);
    config
        .members()
        .forEach(
            m -> info.addMembers(Node.newBuilder().setNodeId(m.id()).setEndpoint(m.endpoint())));
    return info.build();
  }

  static final class ClusterService extends ObjectStoreGrpc.ObjectStoreImplBase {
    private final NodeConfig config;

    ClusterService(NodeConfig config) {
      this.config = config;
    }

    @Override
    public void getClusterInfo(
        GetClusterInfoRequest request, StreamObserver<ClusterInfo> observer) {
      observer.onNext(clusterInfo(config, false));
      observer.onCompleted();
    }
  }
}
