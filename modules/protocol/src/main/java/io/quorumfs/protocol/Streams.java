package io.quorumfs.protocol;

import io.grpc.*;
import io.grpc.stub.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** One message at a time, honoring gRPC transport readiness and an absolute deadline. */
public final class Streams {
  private Streams() {}

  @FunctionalInterface
  public interface Source<T> {
    T next() throws Exception;
  }

  public static long end() {
    Deadline deadline = Context.current().getDeadline();
    long remaining =
        deadline == null
            ? TimeUnit.SECONDS.toNanos(15)
            : Math.min(TimeUnit.SECONDS.toNanos(15), deadline.timeRemaining(TimeUnit.NANOSECONDS));
    return System.nanoTime() + Math.max(0, remaining);
  }

  public static long remaining(long end) {
    long nanos = end - System.nanoTime();
    if (nanos <= 0 || Context.current().isCancelled() || Thread.currentThread().isInterrupted())
      throw Status.DEADLINE_EXCEEDED.asRuntimeException();
    return nanos;
  }

  public static <T> void send(CallStreamObserver<T> stream, T frame, long end)
      throws InterruptedException {
    while (!stream.isReady()) {
      remaining(end);
      if (stream instanceof ServerCallStreamObserver<?> server && server.isCancelled())
        throw Status.CANCELLED.asRuntimeException();
      Thread.sleep(2);
    }
    remaining(end);
    stream.onNext(frame);
  }

  public static <Q, A> A upload(
      Function<StreamObserver<A>, StreamObserver<Q>> open, Source<Q> source, long end)
      throws Exception {
    AtomicReference<ClientCallStreamObserver<Q>> call = new AtomicReference<>();
    CompletableFuture<A> result = new CompletableFuture<>();
    open.apply(
        new ClientResponseObserver<Q, A>() {
          private A reply;

          @Override
          public void beforeStart(ClientCallStreamObserver<Q> stream) {
            call.set(stream);
          }

          @Override
          public void onNext(A value) {
            if (reply != null) result.completeExceptionally(Status.DATA_LOSS.asRuntimeException());
            reply = value;
          }

          @Override
          public void onError(Throwable error) {
            result.completeExceptionally(error);
          }

          @Override
          public void onCompleted() {
            if (reply == null) result.completeExceptionally(Status.DATA_LOSS.asRuntimeException());
            else result.complete(reply);
          }
        });
    try {
      Q frame;
      while ((frame = source.next()) != null) {
        if (result.isDone()) {
          result.get();
          throw Status.DATA_LOSS.asRuntimeException();
        }
        while (!call.get().isReady()) {
          if (result.isDone()) {
            result.get();
            throw Status.DATA_LOSS.asRuntimeException();
          }
          remaining(end);
          Thread.sleep(2);
        }
        remaining(end);
        call.get().onNext(frame);
      }
      call.get().onCompleted();
      return result.get(remaining(end), TimeUnit.NANOSECONDS);
    } finally {
      call.get().cancel("Transfer finished", null);
    }
  }
}
