package io.quorumfs.server;

import io.grpc.*;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import io.quorumfs.replication.PeerClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Development bearer credentials. TLS and per-principal policy arrive in operational hardening. */
public record Access(String clientToken, String peerToken, Set<String> namespaces) {
  public static final Metadata.Key<String> CLIENT_TOKEN =
      Metadata.Key.of("quorumfs-client-token", Metadata.ASCII_STRING_MARSHALLER);

  public Access {
    namespaces = Set.copyOf(namespaces);
    if (clientToken.length() < 32
        || clientToken.length() > 256
        || peerToken.length() < 32
        || peerToken.length() > 256
        || clientToken.equals(peerToken)
        || !clientToken.matches("[a-zA-Z0-9_=-]+")
        || !peerToken.matches("[a-zA-Z0-9_=-]+")
        || namespaces.isEmpty()
        || namespaces.size() > 32
        || namespaces.stream().anyMatch(n -> !n.matches("[a-zA-Z0-9_-]{1,64}")))
      throw new IllegalArgumentException("Invalid bearer credentials or namespace allowlist");
  }

  public static Access environment() {
    String client = System.getenv().getOrDefault("QUORUMFS_CLIENT_TOKEN", "");
    String peer = System.getenv().getOrDefault("QUORUMFS_PEER_TOKEN", "");
    String namespaces = System.getenv().getOrDefault("QUORUMFS_NAMESPACES", "");
    if (client.isEmpty() && peer.isEmpty() && namespaces.isEmpty()) return null;
    return new Access(client, peer, Set.copyOf(Arrays.asList(namespaces.split(",", -1))));
  }

  public ServerInterceptor interceptor(boolean internal) {
    return new ServerInterceptor() {
      @Override
      public <Q, A> ServerCall.Listener<Q> interceptCall(
          ServerCall<Q, A> call, Metadata headers, ServerCallHandler<Q, A> next) {
        if (!internal && call.getMethodDescriptor().getBareMethodName().equals("GetClusterInfo"))
          return next.startCall(call, headers);
        String provided = headers.get(internal ? PeerClient.TOKEN : CLIENT_TOKEN);
        String expected = internal ? peerToken : clientToken;
        if (provided == null
            || !MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.US_ASCII),
                provided.getBytes(StandardCharsets.US_ASCII))) {
          var error = Errors.exception(ErrorReason.ACCESS_DENIED);
          call.close(error.getStatus(), error.getTrailers());
          return new ServerCall.Listener<>() {};
        }
        return next.startCall(call, headers);
      }
    };
  }
}
