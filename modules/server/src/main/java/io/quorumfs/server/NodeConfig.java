package io.quorumfs.server;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** Fixed membership is operator supplied; mismatched persisted identity fails closed. */
public record NodeConfig(
    String clusterId,
    long epoch,
    String nodeId,
    int port,
    Path dataDir,
    List<Member> members,
    int replicas,
    int reads,
    int writes) {
  public record Member(String id, String endpoint) {}

  public NodeConfig {
    members = List.copyOf(members);
    if (!clusterId.matches("[a-zA-Z0-9_-]{1,64}")
        || !nodeId.matches("[a-zA-Z0-9_-]{1,64}")
        || epoch < 1
        || port < 1
        || port > 65535) throw new IllegalArgumentException("Invalid identity, epoch or port");
    if (members.size() != 5)
      throw new IllegalArgumentException("Exactly five fixed members required");
    Set<String> ids = new HashSet<>();
    Set<String> endpoints = new HashSet<>();
    for (Member member : members) {
      if (!member.id().matches("[a-zA-Z0-9_-]{1,64}")
          || !ids.add(member.id())
          || !endpoints.add(member.endpoint())
          || !member.endpoint().matches("[a-zA-Z0-9.-]+:[0-9]{1,5}")) {
        throw new IllegalArgumentException("Invalid or duplicate member");
      }
      int memberPort =
          Integer.parseInt(member.endpoint().substring(member.endpoint().lastIndexOf(':') + 1));
      if (memberPort < 1 || memberPort > 65535)
        throw new IllegalArgumentException("Invalid member port");
    }
    if (!ids.contains(nodeId))
      throw new IllegalArgumentException("Local node is absent from membership");
    if (replicas < 1
        || replicas > 5
        || reads < 1
        || reads > replicas
        || writes < 1
        || writes > replicas
        || reads + writes <= replicas) throw new IllegalArgumentException("Invalid strict quorum");
    dataDir = dataDir.toAbsolutePath().normalize();
  }

  public static NodeConfig load(Path file) throws IOException {
    Properties p = new Properties();
    try (Reader reader = Files.newBufferedReader(file)) {
      p.load(reader);
    }
    Set<String> required =
        Set.of(
            "cluster.id",
            "cluster.epoch",
            "node.id",
            "node.port",
            "data.dir",
            "members",
            "quorum.n",
            "quorum.r",
            "quorum.w",
            "development.insecure");
    if (!p.stringPropertyNames().equals(required))
      throw new IllegalArgumentException("Unknown or missing configuration keys");
    if (!"true".equals(p.getProperty("development.insecure"))) {
      throw new IllegalArgumentException(
          "Q0 requires explicit development.insecure=true; TLS is not implemented");
    }
    List<Member> members =
        java.util.Arrays.stream(p.getProperty("members").split(",", -1))
            .map(
                value -> {
                  String[] fields = value.trim().split("@", -1);
                  if (fields.length != 2)
                    throw new IllegalArgumentException("Member must be id@host:port");
                  return new Member(fields[0], fields[1]);
                })
            .sorted(java.util.Comparator.comparing(Member::id))
            .toList();
    String directory = p.getProperty("data.dir");
    if (directory.isBlank()) throw new IllegalArgumentException("Empty data directory");
    return new NodeConfig(
        p.getProperty("cluster.id"),
        Long.parseLong(p.getProperty("cluster.epoch")),
        p.getProperty("node.id"),
        Integer.parseInt(p.getProperty("node.port")),
        Path.of(directory),
        members,
        Integer.parseInt(p.getProperty("quorum.n")),
        Integer.parseInt(p.getProperty("quorum.r")),
        Integer.parseInt(p.getProperty("quorum.w")));
  }

  public io.quorumfs.ring.HashRing ring() {
    return new io.quorumfs.ring.HashRing(
        clusterId,
        epoch,
        members.stream()
            .map(m -> new io.quorumfs.ring.HashRing.Member(m.id(), m.endpoint()))
            .toList(),
        replicas,
        reads,
        writes);
  }

  public String identity() {
    return "quorumfs-identity-v1\n"
        + clusterId
        + "\n"
        + epoch
        + "\n"
        + nodeId
        + "\n"
        + members
        + "\n"
        + replicas
        + "/"
        + reads
        + "/"
        + writes;
  }
}
