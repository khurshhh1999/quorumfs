package io.quorumfs.replication;

import io.grpc.stub.*;
import io.quorumfs.protocol.*;
import io.quorumfs.protocol.v1.*;
import java.util.concurrent.Semaphore;

/** Manual inbound flow control and exactly-once cleanup for a bounded upload slot. */
public abstract class UploadObserver<Q, A> implements StreamObserver<Q> {
  private static final java.util.concurrent.ScheduledThreadPoolExecutor TIMER = timer();

  private static java.util.concurrent.ScheduledThreadPoolExecutor timer() {
    var timer =
        new java.util.concurrent.ScheduledThreadPoolExecutor(
            1,
            r -> {
              Thread thread = new Thread(r, "upload-deadlines");
              thread.setDaemon(true);
              return thread;
            });
    timer.setRemoveOnCancelPolicy(true);
    return timer;
  }

  private java.util.concurrent.ScheduledFuture<?> expiry;
  protected final ServerCallStreamObserver<A> response;
  protected final long end = Streams.end();
  protected Spool spool;
  private final Semaphore slots;
  private boolean done;

  protected UploadObserver(StreamObserver<A> response, Semaphore slots) {
    this.response = (ServerCallStreamObserver<A>) response;
    this.slots = slots;
    if (!slots.tryAcquire()) throw Errors.exception(ErrorReason.CAPACITY_EXHAUSTED);
    this.response.disableAutoRequest();
    this.response.setOnCancelHandler(() -> fail(io.grpc.Status.CANCELLED.asRuntimeException()));
    this.expiry =
        TIMER.schedule(
            () ->
                Thread.startVirtualThread(
                    () -> fail(io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException())),
            Math.max(1, end - System.nanoTime()),
            java.util.concurrent.TimeUnit.NANOSECONDS);
    this.response.request(1);
  }

  protected abstract void frame(Q value) throws Exception;

  protected abstract A complete() throws Exception;

  @Override
  public final synchronized void onNext(Q value) {
    if (done) return;
    try {
      Streams.remaining(end);
      frame(value);
      response.request(1);
    } catch (Exception e) {
      fail(e);
    }
  }

  @Override
  public final synchronized void onError(Throwable error) {
    fail(error);
  }

  @Override
  public final synchronized void onCompleted() {
    if (done) return;
    try {
      Streams.remaining(end);
      if (spool == null) throw Errors.exception(ErrorReason.INVALID_REQUEST);
      spool.finish();
      A result = complete();
      response.onNext(result);
      response.onCompleted();
      cleanup();
    } catch (Exception e) {
      fail(e);
    }
  }

  private synchronized void fail(Throwable error) {
    if (done) return;
    try {
      if (!response.isCancelled()) response.onError(RpcFailure.map(error));
    } finally {
      cleanup();
    }
  }

  private void cleanup() {
    if (done) return;
    done = true;
    if (expiry != null) expiry.cancel(false);
    try {
      if (spool != null) spool.close();
    } catch (java.io.IOException e) {
      System.err.println("event=transfer_cleanup_failed");
    } finally {
      slots.release();
    }
  }
}
