/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.client.impl;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.client.PrimitiveException;
import io.atomix.client.PrimitiveState;
import io.atomix.client.utils.concurrent.ThreadContext;
import io.grpc.stub.StreamObserver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session operation submitter.
 */
final class PrimitiveSessionExecutor<S> {
  private static final int[] FIBONACCI = new int[]{1, 1, 2, 3, 5};
  private static final Predicate<Throwable> EXCEPTION_PREDICATE = e ->
      e instanceof ConnectException
          || e instanceof TimeoutException
          || e instanceof ClosedChannelException;
  private static final Predicate<Throwable> EXPIRED_PREDICATE = e ->
      e instanceof PrimitiveException.UnknownClient
          || e instanceof PrimitiveException.UnknownSession;
  private static final Predicate<Throwable> CLOSED_PREDICATE = e ->
      e instanceof PrimitiveException.ConcurrentModification
          || e instanceof PrimitiveException.ConcurrentModification;

  private final S service;
  private final PrimitiveSessionState state;
  private final ManagedPrimitiveContext context;
  private final PrimitiveSessionSequencer sequencer;
  private final ThreadContext threadContext;
  private final Map<Long, OperationAttempt> attempts = new LinkedHashMap<>();

  PrimitiveSessionExecutor(
      S service,
      PrimitiveSessionState state,
      ManagedPrimitiveContext context,
      PrimitiveSessionSequencer sequencer,
      ThreadContext threadContext) {
    this.service = checkNotNull(service, "service cannot be null");
    this.state = checkNotNull(state, "state cannot be null");
    this.context = checkNotNull(context, "context cannot be null");
    this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
    this.threadContext = checkNotNull(threadContext, "threadContext cannot be null");
  }

  protected <T> CompletableFuture<T> execute(
      SessionCommandFunction<S, T> function,
      Function<T, SessionResponseHeader> responseHeaderFunction) {
    CompletableFuture<T> future = new CompletableFuture<>();
    threadContext.execute(() -> invokeCommand(function, responseHeaderFunction, future));
    return future;
  }

  protected <T> CompletableFuture<Long> execute(
      SessionCommandFunction<S, T> function,
      Function<T, SessionResponseHeader> responseHeaderFunction,
      StreamObserver<T> observer) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    threadContext.execute(() -> invokeCommand(function, responseHeaderFunction, observer, future));
    return future;
  }

  protected <T> CompletableFuture<T> execute(
      SessionQueryFunction<S, T> function,
      Function<T, SessionResponseHeader> responseHeaderFunction) {
    CompletableFuture<T> future = new CompletableFuture<>();
    threadContext.execute(() -> invokeQuery(function, responseHeaderFunction, future));
    return future;
  }

  protected <T> CompletableFuture<Void> execute(
      SessionQueryFunction<S, T> function,
      Function<T, SessionResponseHeader> responseHeaderFunction,
      StreamObserver<T> observer) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> invokeQuery(function, responseHeaderFunction, observer, future));
    return future;
  }

  /**
   * Submits a command request to the cluster.
   */
  private <T> void invokeCommand(
      RequestFunction<S, SessionCommandHeader, T> requestFunction,
      Function<T, SessionResponseHeader> responseHeaderFunction,
      CompletableFuture<T> future) {
    SessionCommandHeader header = SessionCommandHeader.newBuilder()
        .setSessionId(state.getSessionId())
        .setSequenceNumber(state.nextCommandRequest())
        .build();
    invoke(new CommandAttempt<>(sequencer.nextRequest(), requestFunction, header, responseHeaderFunction, future));
  }

  /**
   * Submits a command request to the cluster.
   */
  private <T> void invokeCommand(
      RequestFunction<S, SessionCommandHeader, T> requestFunction,
      Function<T, SessionResponseHeader> responseHeaderFunction,
      StreamObserver<T> observer,
      CompletableFuture<Long> future) {
    SessionCommandHeader header = SessionCommandHeader.newBuilder()
        .setSessionId(state.getSessionId())
        .setSequenceNumber(state.nextCommandRequest())
        .build();
    invoke(new CommandStreamAttempt<>(sequencer.nextRequest(), requestFunction, header, responseHeaderFunction, observer, future));
  }

  /**
   * Submits a query request to the cluster.
   */
  private <T> void invokeQuery(
      RequestFunction<S, SessionQueryHeader, T> requestFunction,
      Function<T, SessionResponseHeader> responseHeaderFunction,
      CompletableFuture<T> future) {
    SessionQueryHeader header = SessionQueryHeader.newBuilder()
        .setSessionId(state.getSessionId())
        .setLastSequenceNumber(state.getCommandRequest())
        .build();
    invoke(new QueryAttempt<>(sequencer.nextRequest(), requestFunction, header, responseHeaderFunction, future));
  }

  /**
   * Submits a query request to the cluster.
   */
  private <T> void invokeQuery(
      RequestFunction<S, SessionQueryHeader, T> requestFunction,
      Function<T, SessionResponseHeader> responseHeaderFunction,
      StreamObserver<T> observer,
      CompletableFuture<Void> future) {
    SessionQueryHeader header = SessionQueryHeader.newBuilder()
        .setSessionId(state.getSessionId())
        .setLastSequenceNumber(state.getCommandRequest())
        .build();
    invoke(new QueryStreamAttempt<>(sequencer.nextRequest(), requestFunction, header, responseHeaderFunction, observer, future));
  }

  /**
   * Submits an operation attempt.
   *
   * @param attempt The attempt to submit.
   */
  private void invoke(OperationAttempt<?, ?, ?> attempt) {
    if (state.getState() == PrimitiveState.CLOSED) {
      attempt.fail(new PrimitiveException.ConcurrentModification("session closed"));
    } else {
      attempts.put(attempt.id, attempt);
      attempt.send();
      attempt.future.whenComplete((r, e) -> attempts.remove(attempt.id));
    }
  }

  /**
   * Resubmits commands starting after the given sequence number.
   * <p>
   * The sequence number from which to resend commands is the <em>request</em> sequence number, not the client-side
   * sequence number. We resend only commands since queries cannot be reliably resent without losing linearizable
   * semantics. Commands are resent by iterating through all pending operation attempts and retrying commands where the
   * sequence number is greater than the given {@code commandSequence} number and the attempt number is less than or
   * equal to the version.
   */
  private void resubmit(long commandSequence, OperationAttempt attempt) {
    for (Map.Entry<Long, OperationAttempt> entry : attempts.entrySet()) {
      OperationAttempt operation = entry.getValue();
      if (operation instanceof CommandAttempt
          && operation.id > commandSequence
          && operation.attempt <= attempt.attempt) {
        operation.retry();
      }
    }
  }

  /**
   * Resubmits pending commands.
   */
  public void reset() {
    threadContext.execute(() -> {
      for (OperationAttempt attempt : attempts.values()) {
        attempt.retry();
      }
    });
  }

  /**
   * Closes the submitter.
   *
   * @return A completable future to be completed with a list of pending operations.
   */
  public CompletableFuture<Void> close() {
    for (OperationAttempt attempt : new ArrayList<>(attempts.values())) {
      attempt.fail(new PrimitiveException.ConcurrentModification("session closed"));
    }
    attempts.clear();
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Operation attempt.
   */
  private abstract class OperationAttempt<T, U, V> {
    protected final long id;
    protected final RequestFunction<S, T, U> requestFunction;
    protected final T requestHeader;
    protected final Function<U, SessionResponseHeader> responseHeaderFunction;
    protected final int attempt;
    protected final CompletableFuture<V> future;

    protected OperationAttempt(long id, RequestFunction<S, T, U> requestFunction, T requestHeader, Function<U, SessionResponseHeader> responseHeaderFunction, int attempt, CompletableFuture<V> future) {
      this.id = id;
      this.requestFunction = requestFunction;
      this.requestHeader = requestHeader;
      this.responseHeaderFunction = responseHeaderFunction;
      this.attempt = attempt;
      this.future = future;
    }

    /**
     * Returns the header for the given response.
     *
     * @param response the response for which to return the header
     * @return the header for the given response
     */
    protected SessionResponseHeader getHeader(U response) {
      return responseHeaderFunction.apply(response);
    }

    /**
     * Executes the request function.
     *
     * @param observer the response observer
     */
    protected void execute(StreamObserver<U> observer) {
      requestFunction.apply(service, requestHeader, observer);
    }

    /**
     * Executes the request function, returning a single result future.
     *
     * @return the result future
     */
    protected CompletableFuture<U> execute() {
      CompletableFuture<U> future = new CompletableFuture<>();
      execute(new StreamObserver<U>() {
        @Override
        public void onNext(U response) {
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

    /**
     * Sends the attempt.
     */
    protected abstract void send();

    /**
     * Returns the next instance of the attempt.
     *
     * @return The next instance of the attempt.
     */
    protected abstract OperationAttempt<T, U, V> next();

    /**
     * Returns a new instance of the default exception for the operation.
     *
     * @return A default exception for the operation.
     */
    protected abstract Throwable defaultException();

    /**
     * Completes the operation with an exception.
     *
     * @param error The completion exception.
     */
    protected void complete(Throwable error) {
      sequence(null, () -> future.completeExceptionally(error));
    }

    /**
     * Runs the given callback in proper sequence.
     *
     * @param response The operation response.
     * @param callback The callback to run in sequence.
     */
    protected final void sequence(SessionResponseHeader response, Runnable callback) {
      sequencer.sequenceResponse(id, response, callback);
    }

    /**
     * Fails the attempt.
     */
    public void fail() {
      fail(defaultException());
    }

    /**
     * Fails the attempt with the given exception.
     *
     * @param t The exception with which to fail the attempt.
     */
    public void fail(Throwable t) {
      sequence(null, () -> {
        state.setCommandResponse(id);
        future.completeExceptionally(t);
      });

      // If the session has been expired or closed, update the client's state.
      if (EXPIRED_PREDICATE.test(t)) {
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(t)) {
        state.setState(PrimitiveState.CLOSED);
      }
    }

    /**
     * Immediately retries the attempt.
     */
    public void retry() {
      threadContext.execute(() -> invoke(next()));
    }

    /**
     * Retries the attempt after the given duration.
     *
     * @param after The duration after which to retry the attempt.
     */
    public void retry(Duration after) {
      threadContext.schedule(after, () -> invoke(next()));
    }
  }

  /**
   * Command operation attempt.
   */
  private final class CommandAttempt<T> extends OperationAttempt<SessionCommandHeader, T, T> implements BiConsumer<T, Throwable> {

    CommandAttempt(
        long id,
        RequestFunction<S, SessionCommandHeader, T> requestFunction,
        SessionCommandHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        CompletableFuture<T> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, 1, future);
    }

    CommandAttempt(
        long id,
        RequestFunction<S, SessionCommandHeader, T> requestFunction,
        SessionCommandHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        int attempt,
        CompletableFuture<T> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, attempt, future);
    }

    @Override
    protected void send() {
      execute().whenComplete(this);
    }

    @Override
    protected CommandAttempt<T> next() {
      return new CommandAttempt<>(id, requestFunction, requestHeader, responseHeaderFunction, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.CommandFailure("failed to complete command");
    }

    @Override
    public void accept(T response, Throwable error) {
      if (error == null) {
        complete(response);
      } else if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.UnknownSession());
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.ConcurrentModification());
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        fail(error);
      }
    }

    /**
     * Completes the response.
     *
     * @param response the response to complete
     */
    @SuppressWarnings("unchecked")
    protected void complete(T response) {
      SessionResponseHeader header = getHeader(response);
      sequence(header, () -> {
        state.setCommandResponse(id);
        state.setResponseIndex(header.getIndex());
        future.complete(response);
      });
    }
  }

  /**
   * Query operation attempt.
   */
  private final class QueryAttempt<T> extends OperationAttempt<SessionQueryHeader, T, T> implements BiConsumer<T, Throwable> {
    QueryAttempt(
        long id,
        RequestFunction<S, SessionQueryHeader, T> requestFunction,
        SessionQueryHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        CompletableFuture<T> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, 1, future);
    }

    QueryAttempt(
        long id,
        RequestFunction<S, SessionQueryHeader, T> requestFunction,
        SessionQueryHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        int attempt,
        CompletableFuture<T> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, attempt, future);
    }

    @Override
    protected void send() {
      execute().whenComplete(this);
    }

    @Override
    protected QueryAttempt<T> next() {
      return new QueryAttempt<>(id, requestFunction, requestHeader, responseHeaderFunction, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.ConcurrentModification("failed to complete query");
    }

    @Override
    public void accept(T response, Throwable error) {
      if (error == null) {
        complete(response);
      } else if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.UnknownSession());
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.ConcurrentModification());
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.ConcurrentModification("Query failed"));
      } else {
        fail(error);
      }
    }

    /**
     * Completes the given response.
     *
     * @param response the response to complete
     */
    @SuppressWarnings("unchecked")
    protected void complete(T response) {
      SessionResponseHeader header = getHeader(response);
      sequence(header, () -> {
        state.setResponseIndex(header.getIndex());
        future.complete(response);
      });
    }
  }

  /**
   * Command operation attempt.
   */
  private final class CommandStreamAttempt<T>
      extends OperationAttempt<SessionCommandHeader, T, Long>
      implements StreamObserver<T> {
    private final StreamObserver<T> responseObserver;
    private final AtomicBoolean complete = new AtomicBoolean();

    CommandStreamAttempt(
        long id,
        RequestFunction<S, SessionCommandHeader, T> requestFunction,
        SessionCommandHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        StreamObserver<T> responseObserver,
        CompletableFuture<Long> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, 1, future);
      this.responseObserver = responseObserver;
    }

    CommandStreamAttempt(
        long id,
        RequestFunction<S, SessionCommandHeader, T> requestFunction,
        SessionCommandHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        StreamObserver<T> responseObserver,
        int attempt,
        CompletableFuture<Long> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, attempt, future);
      this.responseObserver = responseObserver;
    }

    @Override
    protected void send() {
      execute(this);
    }

    @Override
    protected CommandStreamAttempt<T> next() {
      return new CommandStreamAttempt<>(id, requestFunction, requestHeader, responseHeaderFunction, responseObserver, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.CommandFailure("failed to complete command");
    }

    @Override
    public void onNext(T response) {
      SessionResponseHeader header = getHeader(response);
      if (complete.compareAndSet(false, true)) {
        sequence(header, () -> future.complete(header.getIndex()));
      }
      sequencer.sequenceStream(header.getStreams(0), () -> responseObserver.onNext(response));
    }

    @Override
    public void onCompleted() {
      if (complete.compareAndSet(false, true)) {
        sequence(null, () -> future.complete(null));
      }
      sequencer.closeStream(requestHeader.getSequenceNumber(), () -> responseObserver.onCompleted());
    }

    @Override
    public void onError(Throwable error) {
      if (complete.compareAndSet(false, true)) {
        sequence(null, () -> future.completeExceptionally(error));
      }
      if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        sequencer.closeStream(requestHeader.getSequenceNumber(), () -> responseObserver.onError(new PrimitiveException.UnknownSession()));
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        sequencer.closeStream(requestHeader.getSequenceNumber(), () -> responseObserver.onError(new PrimitiveException.ConcurrentModification()));
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        responseObserver.onError(error);
      }
    }
  }

  /**
   * Query operation attempt.
   */
  private final class QueryStreamAttempt<T>
      extends OperationAttempt<SessionQueryHeader, T, Void>
      implements StreamObserver<T> {
    private final StreamObserver<T> responseObserver;
    private final AtomicBoolean complete = new AtomicBoolean();

    QueryStreamAttempt(
        long id,
        RequestFunction<S, SessionQueryHeader, T> requestFunction,
        SessionQueryHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        StreamObserver<T> responseObserver,
        CompletableFuture<Void> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, 1, future);
      this.responseObserver = responseObserver;
    }

    QueryStreamAttempt(
        long id,
        RequestFunction<S, SessionQueryHeader, T> requestFunction,
        SessionQueryHeader requestHeader,
        Function<T, SessionResponseHeader> responseHeaderFunction,
        StreamObserver<T> responseObserver,
        int attempt,
        CompletableFuture<Void> future) {
      super(id, requestFunction, requestHeader, responseHeaderFunction, attempt, future);
      this.responseObserver = responseObserver;
    }

    @Override
    protected void send() {
      execute(this);
    }

    @Override
    protected QueryStreamAttempt<T> next() {
      return new QueryStreamAttempt<>(id, requestFunction, requestHeader, responseHeaderFunction, responseObserver, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.ConcurrentModification("failed to complete query");
    }

    @Override
    public void onNext(T response) {
      SessionResponseHeader header = getHeader(response);
      if (complete.compareAndSet(false, true)) {
        sequence(null, () -> {
          state.setResponseIndex(header.getIndex());
          future.complete(null);
        });
      }

      sequencer.sequenceStream(header.getStreams(0), () -> {
        state.setResponseIndex(header.getIndex());
        responseObserver.onNext(response);
      });
    }

    @Override
    public void onCompleted() {
      if (complete.compareAndSet(false, true)) {
        sequence(null, () -> future.complete(null));
      }
      responseObserver.onCompleted();
    }

    @Override
    public void onError(Throwable error) {
      if (complete.compareAndSet(false, true)) {
        sequence(null, () -> future.completeExceptionally(error));
      }
      if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        responseObserver.onError(error);
      }
    }
  }
}