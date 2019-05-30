package io.atomix.client.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.atomix.api.headers.Name;
import io.atomix.api.headers.RequestHeader;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.StreamHeader;
import io.atomix.client.AsyncPrimitive;
import io.atomix.client.ManagedAsyncPrimitive;
import io.atomix.client.PrimitiveState;
import io.atomix.client.utils.concurrent.Futures;
import io.atomix.client.utils.concurrent.Scheduled;
import io.atomix.client.utils.concurrent.ThreadContext;
import io.grpc.stub.StreamObserver;

/**
 * Primitive session.
 */
public abstract class AbstractManagedPrimitive<S, P extends AsyncPrimitive> extends AbstractAsyncPrimitive<S, P> implements ManagedAsyncPrimitive<P> {
  private static final double TIMEOUT_FACTOR = .5;
  private static final long MIN_TIMEOUT_DELTA = 2500;

  private final Duration timeout;
  private final AtomicBoolean open = new AtomicBoolean();
  private PrimitiveSessionState state;
  private PrimitiveSessionSequencer sequencer;
  private PrimitiveSessionExecutor<S> executor;
  private Scheduled keepAliveTimer;

  protected AbstractManagedPrimitive(
      Name name,
      S service,
      ThreadContext context,
      Duration timeout) {
    super(name, service, context);
    this.timeout = timeout;
  }

  private RequestHeader getSessionHeader() {
    return RequestHeader.newBuilder()
        .setName(getName())
        .setSessionId(state.getSessionId())
        .setSequenceNumber(state.getCommandResponse())
        .addAllStreams(sequencer.streams().stream()
            .map(stream -> StreamHeader.newBuilder()
                .setStreamId(stream.streamId())
                .setIndex(stream.getStreamIndex())
                .setLastItemNumber(stream.getStreamSequence())
                .build())
            .collect(Collectors.toList()))
        .build();
  }

  protected <T> CompletableFuture<T> session(BiConsumer<RequestHeader, StreamObserver<T>> function) {
    CompletableFuture<T> future = new CompletableFuture<>();
    function.accept(getSessionHeader(), new StreamObserver<T>() {
      @Override
      public void onNext(T response) {
        future.complete(response);
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
      }
    });
    return future;
  }

  protected <T> CompletableFuture<T> command(
      BiConsumer<RequestHeader, StreamObserver<T>> function,
      Function<T, ResponseHeader> headerFunction) {
    return executor.executeCommand(function, headerFunction);
  }

  protected <T> CompletableFuture<Long> command(
      BiConsumer<RequestHeader, StreamObserver<T>> function,
      Function<T, ResponseHeader> headerFunction,
      StreamObserver<T> handler) {
    return executor.executeCommand(function, headerFunction, handler);
  }

  protected <T> CompletableFuture<T> query(
      BiConsumer<RequestHeader, StreamObserver<T>> function,
      Function<T, ResponseHeader> headerFunction) {
    return executor.executeQuery(function, headerFunction);
  }

  protected <T> CompletableFuture<Void> query(
      BiConsumer<RequestHeader, StreamObserver<T>> function,
      Function<T, ResponseHeader> headerFunction,
      StreamObserver<T> handler) {
    return executor.executeQuery(function, headerFunction, handler);
  }

  protected void state(Consumer<PrimitiveState> consumer) {
    state.addStateChangeListener(consumer);
  }

  protected PrimitiveState getState() {
    return state.getState();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<P> connect() {
    if (!open.compareAndSet(false, true)) {
      return Futures.exceptionalFuture(new IllegalStateException());
    }
    return openSession(timeout).thenApply(sessionId -> {
      ManagedPrimitiveContext context = new ManagedPrimitiveContext(
          sessionId,
          name(),
          type(),
          timeout);
      state = new PrimitiveSessionState(getName(), sessionId, timeout.toMillis());
      sequencer = new PrimitiveSessionSequencer(state, context);
      executor = new PrimitiveSessionExecutor<>(getService(), state, context, sequencer, context());
      keepAlive(System.currentTimeMillis());
      return (P) this;
    });
  }

  /**
   * Opens the primitive session.
   *
   * @param timeout the session timeout
   * @return a future to be completed with the session ID
   */
  protected abstract CompletableFuture<Long> openSession(Duration timeout);

  /**
   * Keeps the primitive session alive.
   */
  protected void keepAlive(long lastKeepAliveTime) {
    long keepAliveTime = System.currentTimeMillis();
    keepAlive().whenComplete((succeeded, error) -> {
      if (open.get()) {
        long delta = System.currentTimeMillis() - keepAliveTime;
        // If the keep-alive succeeded, ensure the session state is CONNECTED and schedule another keep-alive.
        if (error == null) {
          if (succeeded) {
            state.setState(PrimitiveState.CONNECTED);
            scheduleKeepAlive(System.currentTimeMillis(), delta);
          } else {
            state.setState(PrimitiveState.EXPIRED);
          }
        }
        // If the keep-alive failed, set the session state to SUSPENDED and schedule another keep-alive.
        else {
          state.setState(PrimitiveState.SUSPENDED);
          scheduleKeepAlive(lastKeepAliveTime, delta);
        }
      }
    });
  }

  /**
   * Sends a keep-alive request to the cluster.
   *
   * @return a future indicating whether the request was successful
   */
  protected abstract CompletableFuture<Boolean> keepAlive();

  /**
   * Schedules a keep-alive request.
   */
  private synchronized void scheduleKeepAlive(long lastKeepAliveTime, long delta) {
    if (keepAliveTimer != null) {
      keepAliveTimer.cancel();
    }

    Duration delay = Duration.ofMillis(
        Math.max(Math.max((long) (timeout.toMillis() * TIMEOUT_FACTOR) - delta,
            timeout.toMillis() - MIN_TIMEOUT_DELTA - delta), 0));
    keepAliveTimer = context().schedule(delay, () -> {
      if (open.get()) {
        keepAlive(lastKeepAliveTime);
      }
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open.compareAndSet(true, false)) {
      return Futures.exceptionalFuture(new IllegalStateException());
    }
    keepAliveTimer.cancel();
    return close(false);
  }

  /**
   * Sends a close request to the cluster.
   *
   * @param delete whether to delete the service
   * @return a future to be completed once the close is complete
   */
  protected abstract CompletableFuture<Void> close(boolean delete);

  @Override
  public CompletableFuture<Void> delete() {
    return close(true);
  }
}
