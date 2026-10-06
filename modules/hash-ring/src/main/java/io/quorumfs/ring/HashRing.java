package io.quorumfs.ring;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Fixed SHA-256/128 ring. Canonical bytes include the complete virtual-node map. */
public final class HashRing {
  public static final int VIRTUAL_NODES = 128;

  public record Member(String id, String endpoint) {}

  private record Token(String position, String node, int index) {}

  private final String cluster;
  private final long epoch;
  private final List<Member> members;
  private final int replicas, reads, writes;
  private final List<Token> tokens;
  private final byte[] canonical;

  public HashRing(
      String cluster, long epoch, List<Member> members, int replicas, int reads, int writes) {
    if (cluster == null || !cluster.matches("[a-zA-Z0-9_-]{1,64}") || epoch < 1)
      throw new IllegalArgumentException("Invalid cluster or epoch");
    this.cluster = cluster;
    this.epoch = epoch;
    this.members = members.stream().sorted(Comparator.comparing(Member::id)).toList();
    Set<String> ids = new HashSet<>(), endpoints = new HashSet<>();
    if (members.size() != 5) throw new IllegalArgumentException("Exactly five members required");
    for (Member member : this.members) {
      if (!member.id().matches("[a-zA-Z0-9_-]{1,64}")
          || !ids.add(member.id())
          || !member.endpoint().matches("[a-zA-Z0-9.-]+:[0-9]{1,5}")
          || member.endpoint().length() > 253
          || !endpoints.add(member.endpoint()))
        throw new IllegalArgumentException("Invalid or duplicate member");
      int port =
          Integer.parseInt(member.endpoint().substring(member.endpoint().lastIndexOf(':') + 1));
      if (port < 1 || port > 65535) throw new IllegalArgumentException("Invalid member port");
    }
    if (replicas < 1
        || replicas > 5
        || reads < 1
        || writes < 1
        || reads > replicas
        || writes > replicas
        || reads + writes <= replicas) throw new IllegalArgumentException("Invalid strict quorum");
    this.replicas = replicas;
    this.reads = reads;
    this.writes = writes;
    List<Token> positions = new ArrayList<>();
    for (Member member : this.members) {
      for (int i = 0; i < VIRTUAL_NODES; i++) {
        positions.add(
            new Token(
                hash("vnode", member.id(), ByteBuffer.allocate(4).putInt(i).array()),
                member.id(),
                i));
      }
    }
    positions.sort(
        Comparator.comparing(Token::position)
            .thenComparing(Token::node)
            .thenComparingInt(Token::index));
    tokens = List.copyOf(positions);
    StringBuilder text =
        new StringBuilder("quorumfs-ring-v1\n")
            .append(cluster)
            .append('\n')
            .append(epoch)
            .append('\n')
            .append(replicas)
            .append(' ')
            .append(reads)
            .append(' ')
            .append(writes)
            .append('\n');
    this.members.forEach(m -> text.append(m.id()).append(' ').append(m.endpoint()).append('\n'));
    tokens.forEach(
        t ->
            text.append(t.position())
                .append(' ')
                .append(t.node())
                .append(' ')
                .append(t.index())
                .append('\n'));
    canonical = text.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static String hash(String domain, String namespace, byte[] key) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(domain.getBytes(StandardCharsets.US_ASCII));
      byte[] ns = namespace.getBytes(StandardCharsets.UTF_8);
      digest.update(ByteBuffer.allocate(4).putInt(ns.length).array());
      digest.update(ns);
      digest.update(ByteBuffer.allocate(4).putInt(key.length).array());
      digest.update(key);
      return HexFormat.of().formatHex(digest.digest(), 0, 16);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  public static String keyHash(String namespace, byte[] key) {
    if (namespace == null
        || !namespace.matches("[a-zA-Z0-9_-]{1,64}")
        || key == null
        || key.length < 1
        || key.length > 1024) throw new IllegalArgumentException("Invalid object key");
    return hash("object", namespace, key);
  }

  public List<String> owners(String namespace, byte[] key) {
    String position = keyHash(namespace, key);
    int low = 0, high = tokens.size();
    while (low < high) {
      int mid = (low + high) >>> 1;
      if (tokens.get(mid).position().compareTo(position) < 0) low = mid + 1;
      else high = mid;
    }
    Set<String> owners = new LinkedHashSet<>();
    for (int i = 0; i < tokens.size() && owners.size() < replicas; i++)
      owners.add(tokens.get((low + i) % tokens.size()).node());
    return List.copyOf(owners);
  }

  public byte[] serialize() {
    return canonical.clone();
  }

  public String fingerprint() {
    return hash("config", cluster, canonical);
  }

  public Set<String> nodeIds() {
    Set<String> ids = new TreeSet<>();
    members.forEach(m -> ids.add(m.id()));
    return Collections.unmodifiableSet(ids);
  }

  public long epoch() {
    return epoch;
  }

  public String cluster() {
    return cluster;
  }

  public void requireCompatible(HashRing peer) {
    if (!Arrays.equals(canonical, peer.canonical))
      throw new IllegalArgumentException("Cluster, epoch or ring configuration mismatch");
  }

  public static HashRing deserialize(byte[] bytes) {
    if (bytes == null || bytes.length > 100000)
      throw new IllegalArgumentException("Invalid ring encoding");
    String[] lines = new String(bytes, StandardCharsets.UTF_8).split("\n", -1);
    if (lines.length != 650 || !lines[0].equals("quorumfs-ring-v1"))
      throw new IllegalArgumentException("Invalid ring encoding");
    String[] quorum = lines[3].split(" ", -1);
    if (quorum.length != 3) throw new IllegalArgumentException("Invalid quorum encoding");
    List<Member> members = new ArrayList<>();
    for (int i = 4; i < 9; i++) {
      String[] member = lines[i].split(" ", -1);
      if (member.length != 2) throw new IllegalArgumentException("Invalid member encoding");
      members.add(new Member(member[0], member[1]));
    }
    HashRing ring =
        new HashRing(
            lines[1],
            Long.parseLong(lines[2]),
            members,
            Integer.parseInt(quorum[0]),
            Integer.parseInt(quorum[1]),
            Integer.parseInt(quorum[2]));
    if (!Arrays.equals(bytes, ring.canonical))
      throw new IllegalArgumentException("Noncanonical or corrupt vnode map");
    return ring;
  }
}
