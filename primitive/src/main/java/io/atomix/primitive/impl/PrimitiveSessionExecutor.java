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
import java.util.function.Predicate;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionResponseContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session operation submitter.
 */
final class PrimitiveSessionExecutor<P extends SessionEnabledPrimitiveProxy> {
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

  PrimitiveSessionExecutor(
      P proxy,
      PrimitiveSessionState state,
      ManagedPrimitiveContext context,
      PrimitiveSessionSequencer sequencer) {
    this.proxy = checkNotNull(proxy, "proxy cannot be null");
    this.state = checkNotNull(state, "state");
    this.context = checkNotNull(context, "context cannot be null");
    this.sequencer = checkNotNull(sequencer, "sequencer");
  }

  protected <T, U> CompletableFuture<U> execute(
      SessionCommandFunction<P, T, U> function,
      T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    proxy.context().execute(() -> invokeCommand(function, request, future));
    return future;
  }

  protected <T, U> CompletableFuture<Long> execute(
      SessionCommandStreamFunction<P, T, U> function,
      T request,
      StreamHandler<U> handler) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    proxy.context().execute(() -> invokeCommand(function, request, handler, future));
    return future;
  }

  protected <T, U> CompletableFuture<U> execute(
      SessionQueryFunction<P, T, U> function,
      T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    proxy.context().execute(() -> invokeQuery(function, request, future));
    return future;
  }

  protected <T, U> CompletableFuture<Void> execute(
      SessionQueryStreamFunction<P, T, U> function,
      T request,
      StreamHandler<U> handler) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    proxy.context().execute(() -> invokeQuery(function, request, handler, future));
    return future;
  }

  /**
   * Submits a command request to the cluster.
   */
  private <T, U> void invokeCommand(SessionCommandFunction<P, T, U> command, T request, CompletableFuture<U> future) {
    SessionCommandContext context = SessionCommandContext.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setSequenceNumber(state.nextCommandRequest())
        .build();
    invoke(new CommandAttempt<>(sequencer.nextRequest(), command, context, request, future));
  }

  /**
   * Submits a command request to the cluster.
   */
  private <T, U> void invokeCommand(
      SessionCommandStreamFunction<P, T, U> command,
      T request,
      StreamHandler<U> handler,
      CompletableFuture<Long> future) {
    SessionCommandContext context = SessionCommandContext.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setSequenceNumber(state.nextCommandRequest())
        .build();
    invoke(new CommandStreamAttempt<>(sequencer.nextRequest(), command, context, request, handler, future));
  }

  /**
   * Submits a query request to the cluster.
   */
  private <T, U> void invokeQuery(SessionQueryFunction<P, T, U> query, T request, CompletableFuture<U> future) {
    SessionQueryContext context = SessionQueryContext.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setLastSequenceNumber(state.getCommandRequest())
        .build();
    invoke(new QueryAttempt<>(sequencer.nextRequest(), query, context, request, future));
  }

  /**
   * Submits a query request to the cluster.
   */
  private <T, U> void invokeQuery(
      SessionQueryStreamFunction<P, T, U> query,
      T request,
      StreamHandler<U> handler,
      CompletableFuture<Void> future) {
    SessionQueryContext context = SessionQueryContext.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setLastSequenceNumber(state.getCommandRequest())
        .build();
    invoke(new QueryStreamAttempt<>(sequencer.nextRequest(), query, context, request, handler, future));
  }

  /**
   * Submits an operation attempt.
   *
   * @param attempt The attempt to submit.
   */
  private void invoke(OperationAttempt<?, ?, ?> attempt) {
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
  private abstract class OperationAttempt<T, U, V> {
    protected final long id;
    protected final T context;
    protected final U request;
    protected final int attempt;
    protected final CompletableFuture<V> future;

    protected OperationAttempt(long id, T context, U request, int attempt, CompletableFuture<V> future) {
      this.id = id;
      this.context = context;
      this.request = request;
      this.attempt = attempt;
      this.future = future;
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
    protected final void sequence(SessionResponseContext response, Runnable callback) {
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
  private final class CommandAttempt<T, U> extends OperationAttempt<SessionCommandContext, T, U> implements BiConsumer<Pair<SessionResponseContext, U>, Throwable> {
    private final SessionCommandFunction<P, T, U> function;

    CommandAttempt(
        long id,
        SessionCommandFunction<P, T, U> function,
        SessionCommandContext context,
        T request,
        CompletableFuture<U> future) {
      super(id, context, request, 1, future);
      this.function = function;
    }

    CommandAttempt(
        long id,
        SessionCommandFunction<P, T, U> function,
        SessionCommandContext context,
        T request,
        int attempt,
        CompletableFuture<U> future) {
      super(id, context, request, attempt, future);
      this.function = function;
    }

    @Override
    protected void send() {
      function.execute(proxy, context, request).whenComplete(this);
    }

    @Override
    protected CommandAttempt<T, U> next() {
      return new CommandAttempt<>(id, function, context, request, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.CommandFailure("failed to complete command");
    }

    @Override
    public void accept(Pair<SessionResponseContext, U> response, Throwable error) {
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

    /**
     * Completes the response.
     *
     * @param response the response to complete
     */
    @SuppressWarnings("unchecked")
    protected void complete(Pair<SessionResponseContext, U> response) {
      sequence(response.getLeft(), () -> {
        state.setCommandResponse(id);
        state.setResponseIndex(response.getLeft().getIndex());
        future.complete(response.getRight());
      });
    }
  }

  /**
   * Query operation attempt.
   */
  private final class QueryAttempt<T, U> extends OperationAttempt<SessionQueryContext, T, U> implements BiConsumer<Pair<SessionResponseContext, U>, Throwable> {
    private final SessionQueryFunction<P, T, U> function;

    QueryAttempt(
        long id,
        SessionQueryFunction<P, T, U> function,
        SessionQueryContext context,
        T request,
        CompletableFuture<U> future) {
      super(id, context, request, 1, future);
      this.function = function;
    }

    QueryAttempt(
        long id,
        SessionQueryFunction<P, T, U> function,
        SessionQueryContext context,
        T request,
        int attempt,
        CompletableFuture<U> future) {
      super(id, context, request, attempt, future);
      this.function = function;
    }

    @Override
    protected void send() {
      function.execute(proxy, context, request).whenComplete(this);
    }

    @Override
    protected QueryAttempt<T, U> next() {
      return new QueryAttempt<>(id, function, context, request, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.QueryFailure("failed to complete query");
    }

    @Override
    public void accept(Pair<SessionResponseContext, U> response, Throwable error) {
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

    /**
     * Completes the given response.
     *
     * @param response the response to complete
     */
    @SuppressWarnings("unchecked")
    protected void complete(Pair<SessionResponseContext, U> response) {
      sequence(response.getLeft(), () -> {
        state.setResponseIndex(response.getLeft().getIndex());
        future.complete(response.getRight());
      });
    }
  }

  /**
   * Command operation attempt.
   */
  private final class CommandStreamAttempt<T, U>
      extends OperationAttempt<SessionCommandContext, T, Long>
      implements StreamHandler<Pair<SessionStreamContext, U>>, BiConsumer<SessionResponseContext, Throwable> {
    private final SessionCommandStreamFunction<P, T, U> function;
    private final StreamHandler<U> handler;

    CommandStreamAttempt(
        long id,
        SessionCommandStreamFunction<P, T, U> function,
        SessionCommandContext context,
        T request,
        StreamHandler<U> handler,
        CompletableFuture<Long> future) {
      super(id, context, request, 1, future);
      this.function = function;
      this.handler = handler;
    }

    CommandStreamAttempt(
        long id,
        SessionCommandStreamFunction<P, T, U> function,
        SessionCommandContext context,
        T request,
        StreamHandler<U> handler,
        int attempt,
        CompletableFuture<Long> future) {
      super(id, context, request, attempt, future);
      this.function = function;
      this.handler = handler;
    }

    @Override
    protected void send() {
      function.execute(proxy, context, request, this).whenComplete(this);
    }

    @Override
    protected CommandStreamAttempt<T, U> next() {
      return new CommandStreamAttempt<>(id, function, context, request, handler, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.CommandFailure("failed to complete command");
    }

    @Override
    public void next(Pair<SessionStreamContext, U> response) {
      sequencer.sequenceStream(response.getLeft(), () -> handler.next(response.getRight()));
    }

    @Override
    public void complete() {
      sequencer.closeStream(context.getSequenceNumber(), () -> handler.complete());
    }

    @Override
    public void error(Throwable error) {
      if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        sequencer.closeStream(context.getSequenceNumber(), () -> handler.error(new PrimitiveException.UnknownSession()));
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        sequencer.closeStream(context.getSequenceNumber(), () -> handler.error(new PrimitiveException.UnknownService()));
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        handler.error(error);
      }
    }

    @Override
    public void accept(SessionResponseContext response, Throwable error) {
      if (error == null) {
        // Sequence the response and complete the future with the requested stream ID.
        sequencer.sequenceResponse(id, response, () -> response.getStreamsList().stream()
            .filter(s -> s.getIndex() == response.getIndex())
            .findFirst()
            .ifPresent(s -> future.complete(s.getStreamId())));
      } else {
        future.completeExceptionally(error);
      }
    }
  }

  /**
   * Query operation attempt.
   */
  private final class QueryStreamAttempt<T, U>
      extends OperationAttempt<SessionQueryContext, T, Void>
      implements StreamHandler<Pair<SessionStreamContext, U>>, BiConsumer<SessionResponseContext, Throwable> {
    private final SessionQueryStreamFunction<P, T, U> function;
    private final StreamHandler<U> handler;

    QueryStreamAttempt(
        long id,
        SessionQueryStreamFunction<P, T, U> function,
        SessionQueryContext context,
        T request,
        StreamHandler<U> handler,
        CompletableFuture<Void> future) {
      super(id, context, request, 1, future);
      this.function = function;
      this.handler = handler;
    }

    QueryStreamAttempt(
        long id,
        SessionQueryStreamFunction<P, T, U> function,
        SessionQueryContext context,
        T request,
        StreamHandler<U> handler,
        int attempt,
        CompletableFuture<Void> future) {
      super(id, context, request, attempt, future);
      this.function = function;
      this.handler = handler;
    }

    @Override
    protected void send() {
      function.execute(proxy, context, request, this).whenComplete(this);
    }

    @Override
    protected QueryStreamAttempt<T, U> next() {
      return new QueryStreamAttempt<>(id, function, context, request, handler, this.attempt + 1, future);
    }

    @Override
    protected Throwable defaultException() {
      return new PrimitiveException.QueryFailure("failed to complete query");
    }

    @Override
    public void next(Pair<SessionStreamContext, U> response) {
      handler.next(response.getRight());
    }

    @Override
    public void complete() {
      handler.complete();
    }

    @Override
    public void error(Throwable error) {
      if (EXPIRED_PREDICATE.test(error) || (error instanceof CompletionException && EXPIRED_PREDICATE.test(error.getCause()))) {
        state.setState(PrimitiveState.EXPIRED);
      } else if (CLOSED_PREDICATE.test(error) || (error instanceof CompletionException && CLOSED_PREDICATE.test(error.getCause()))) {
        state.setState(PrimitiveState.CLOSED);
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt - 1, FIBONACCI.length - 1)]));
      } else {
        handler.error(error);
      }
    }

    @Override
    public void accept(SessionResponseContext response, Throwable error) {
      if (error == null) {
        sequence(response, () -> {
          state.setResponseIndex(response.getIndex());
          future.complete(null);
        });
      } else {
        future.completeExceptionally(error);
      }
    }
  }

}
