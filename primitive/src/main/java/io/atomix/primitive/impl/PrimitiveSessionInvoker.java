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
package io.atomix.primitive.impl;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import org.apache.commons.lang3.tuple.Pair;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session operation submitter.
 */
final class PrimitiveSessionInvoker<P extends SessionEnabledPrimitiveProxy> {
  private static final int[] FIBONACCI = new int[]{1, 1, 2, 3, 5};
  private static final Predicate<Throwable> EXCEPTION_PREDICATE = e ->
      e instanceof ConnectException
          || e instanceof TimeoutException
          || e instanceof ClosedChannelException;
  private static final Predicate<Throwable> EXPIRED_PREDICATE = e ->
      e instanceof PrimitiveException.UnknownClient
          || e instanceof PrimitiveException.UnknownSession;
  private static final Predicate<Throwable> CLOSED_PREDICATE = e ->
      e instanceof PrimitiveException.ClosedSession
          || e instanceof PrimitiveException.UnknownService;

  private final P proxy;
  private final PrimitiveSessionState state;
  private final ManagedPrimitiveContext context;
  private final PrimitiveSessionSequencer sequencer;
  private final Map<Long, OperationAttempt> attempts = new LinkedHashMap<>();

  PrimitiveSessionInvoker(
      P proxy,
      PrimitiveSessionState state,
      ManagedPrimitiveContext context,
      PrimitiveSessionSequencer sequencer) {
    this.proxy = checkNotNull(proxy, "proxy cannot be null");
    this.state = checkNotNull(state, "state");
    this.context = checkNotNull(context, "context cannot be null");
    this.sequencer = checkNotNull(sequencer, "sequencer");
  }

  public <T> CompletableFuture<T> command(BiFunction<P, SessionCommandContext, CompletableFuture<Pair<SessionContext, T>>> function) {
    CompletableFuture<T> future = new CompletableFuture<>();
    proxy.context().execute(() -> invokeCommand(function, future));
    return future;
  }

  public <T> CompletableFuture<T> query(BiFunction<P, SessionQueryContext, CompletableFuture<Pair<SessionContext, T>>> function) {
    CompletableFuture<T> future = new CompletableFuture<>();
    proxy.context().execute(() -> invokeQuery(function, future));
    return future;
  }

  /**
   * Submits a command request to the cluster.
   */
  private <T> void invokeCommand(BiFunction<P, SessionCommandContext, CompletableFuture<Pair<SessionContext, T>>> command, CompletableFuture<T> future) {
    SessionCommandContext context = SessionCommandContext.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setSequenceNumber(state.nextCommandRequest())
        .build();
    invoke(new CommandAttempt<>(sequencer.nextRequest(), context, command, future));
  }

  /**
   * Submits a query request to the cluster.
   */
  private <T> void invokeQuery(BiFunction<P, SessionQueryContext, CompletableFuture<Pair<SessionContext, T>>> query, CompletableFuture<T> future) {
    SessionQueryContext context = SessionQueryContext.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setLastSequenceNumber(state.getCommandRequest())
        .build();
    invoke(new QueryAttempt<>(sequencer.nextRequest(), context, query, future));
  }

  /**
   * Submits an operation attempt.
   *
   * @param attempt The attempt to submit.
   */
  private <T> void invoke(OperationAttempt<?, T> attempt) {
    if (state.getState() == PrimitiveState.CLOSED) {
      attempt.fail(new PrimitiveException.ClosedSession("session closed"));
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
    proxy.context().execute(() -> {
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
      attempt.fail(new PrimitiveException.ClosedSession("session closed"));
    }
    attempts.clear();
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Operation attempt.
   */
  private abstract class OperationAttempt<T, U> implements BiConsumer<Pair<SessionContext, U>, Throwable> {
    protected final long id;
    protected final T context;
    protected final int attempt;
    protected final BiFunction<P, T, CompletableFuture<Pair<SessionContext, U>>> operation;
    protected final CompletableFuture<U> future;

    protected OperationAttempt(long id, T context, int attempt, BiFunction<P, T, CompletableFuture<Pair<SessionContext, U>>> operation, CompletableFuture<U> future) {
      this.id = id;
      this.context = context;
      this.attempt = attempt;
      this.operation = operation;
      this.future = future;
    }

    /**
     * Sends the attempt.
     */
    protected void send() {
      operation.apply(proxy, context);
    }

    /**
     * Returns the next instance of the attempt.
     *
     * @return The next instance of the attempt.
     */
    protected abstract OperationAttempt<T, U> next();

    /**
     * Returns a new instance of the default exception for the operation.
     *
     * @return A default exception for the operation.
     */
    protected abstract Throwable defaultException();

    /**
     * Completes the operation successfully.
     *
     * @param response The operation response.
     */
    protected abstract void complete(Pair<SessionContext, U> response);

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
    protected final void sequence(Pair<SessionContext, U> response, Runnable callback) {
      sequencer.sequenceResponse(id, response.getLeft(), callback);
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
      proxy.context().execute(() -> invoke(next()));
    }

    /**
     * Retries the attempt after the given duration.
     *
     * @param after The duration after which to retry the attempt.
     */
    public void retry(Duration after) {
      proxy.context().schedule(after, () -> invoke(next()));
    }
  }

  /**
   * Command operation attempt.
   */
  private final class CommandAttempt<T> extends OperationAttempt<SessionCommandContext, T> {
    CommandAttempt(
        long id,
        SessionCommandContext context,
        BiFunction<P, SessionCommandContext, CompletableFuture<Pair<SessionContext, T>>> command,
        CompletableFuture<T> future) {
      super(id, context, 1, command, future);
    }

    CommandAttempt(
        long id,
        SessionCommandContext context,
        int attempt,
        BiFunction<P, SessionCommandContext, CompletableFuture<Pair<SessionContext, T>>> command,
        CompletableFuture<T> future) {
      super(id, context, attempt, command, future);
    }

    @Override
    protected CommandAttempt<T> next() {
      return new CommandAttempt<>(id, context, this.attempt + 1, operation, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.CommandFailure("failed to complete command");
    }

    @Override
    public void accept(Pair<SessionContext, T> response, Throwable error) {
      if (error == null) {
        complete(response);
      } else if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.UnknownSession());
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.UnknownService());
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        fail(error);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void complete(Pair<SessionContext, T> response) {
      sequence(response, () -> {
        state.setCommandResponse(id);
        state.setResponseIndex(response.getLeft().getIndex());
        future.complete(response.getRight());
      });
    }
  }

  /**
   * Query operation attempt.
   */
  private final class QueryAttempt<T> extends OperationAttempt<SessionQueryContext, T> {
    QueryAttempt(
        long id,
        SessionQueryContext context,
        BiFunction<P, SessionQueryContext, CompletableFuture<Pair<SessionContext, T>>> query,
        CompletableFuture<T> future) {
      super(id, context, 1, query, future);
    }

    QueryAttempt(
        long id,
        SessionQueryContext context,
        int attempt,
        BiFunction<P, SessionQueryContext, CompletableFuture<Pair<SessionContext, T>>> query,
        CompletableFuture<T> future) {
      super(id, context, attempt, query, future);
    }

    @Override
    protected QueryAttempt<T> next() {
      return new QueryAttempt<>(id, context, this.attempt + 1, operation, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.QueryFailure("failed to complete query");
    }

    @Override
    public void accept(Pair<SessionContext, T> response, Throwable error) {
      if (error == null) {
        complete(response);
      } else if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.UnknownSession());
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.UnknownService());
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        complete(new PrimitiveException.QueryFailure("Query failed"));
      } else {
        fail(error);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void complete(Pair<SessionContext, T> response) {
      sequence(response, () -> {
        state.setResponseIndex(response.getLeft().getIndex());
        future.complete(response.getRight());
      });
    }
  }

}
