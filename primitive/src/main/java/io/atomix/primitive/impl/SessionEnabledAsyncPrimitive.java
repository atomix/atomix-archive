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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.ManagedAsyncPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.KeepAliveRequest;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Session enabled asynchronous primitive.
 */
public abstract class SessionEnabledAsyncPrimitive<P extends SessionEnabledPrimitiveProxy, T extends AsyncPrimitive>
    extends SimpleAsyncPrimitive<P>
    implements ManagedAsyncPrimitive<T> {
  private static final double TIMEOUT_FACTOR = .5;
  private static final long MIN_TIMEOUT_DELTA = 2500;

  private final Duration timeout;
  private final PrimitiveManagementService managementService;
  private final AtomicBoolean open = new AtomicBoolean();
  private PrimitiveSessionState state;
  private PrimitiveSessionSequencer sequencer;
  private PrimitiveSessionExecutor<P> executor;
  private Scheduled keepAliveTimer;

  public SessionEnabledAsyncPrimitive(
      P proxy,
      Duration timeout,
      PrimitiveManagementService managementService) {
    super(proxy);
    this.timeout = timeout;
    this.managementService = managementService;
  }

  protected <T, U> CompletableFuture<U> execute(
      SessionCommandFunction<P, T, U> function,
      T request) {
    return executor.execute(function, request);
  }

  protected <T, U> CompletableFuture<Long> execute(
      SessionCommandStreamFunction<P, T, U> function,
      T request,
      StreamHandler<U> handler) {
    return executor.execute(function, request, handler);
  }

  protected <T, U> CompletableFuture<U> execute(
      SessionQueryFunction<P, T, U> function,
      T request) {
    return executor.execute(function, request);
  }

  protected <T, U> CompletableFuture<Void> execute(
      SessionQueryStreamFunction<P, T, U> function,
      T request,
      StreamHandler<U> handler) {
    return executor.execute(function, request, handler);
  }

  protected void state(Consumer<PrimitiveState> consumer) {
    state.addStateChangeListener(consumer);
  }

  protected PrimitiveState getState() {
    return state.getState();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> connect() {
    if (!open.compareAndSet(false, true)) {
      return Futures.exceptionalFuture(new IllegalStateException());
    }
    return managementService.getSessionIdService().nextSessionId()
        .thenCompose(sessionId -> getProxy().openSession(OpenSessionRequest.newBuilder()
            .setSessionId(sessionId.id())
            .setTimeout(timeout.toMillis())
            .build())
            .thenApply(response -> {
              ManagedPrimitiveContext context = new ManagedPrimitiveContext(
                  managementService.getMembershipService().getLocalMember().id(),
                  sessionId,
                  name(),
                  type(),
                  timeout);
              state = new PrimitiveSessionState(sessionId, timeout.toMillis());
              sequencer = new PrimitiveSessionSequencer(state, context);
              executor = new PrimitiveSessionExecutor<>(getProxy(), state, context, sequencer);
              keepAlive(System.currentTimeMillis());
              return (T) this;
            }));
  }

  /**
   * Keeps the primitive session alive.
   */
  protected void keepAlive(long lastKeepAliveTime) {
    long keepAliveTime = System.currentTimeMillis();
    getProxy().keepAlive(KeepAliveRequest.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setCommandSequence(state.getCommandResponse())
        .putAllStreams(sequencer.streams().stream()
            .map(stream -> Pair.of(stream.streamId(), stream.getStreamSequence()))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue)))
        .build())
        .whenComplete((response, error) -> {
          if (open.get()) {
            long delta = System.currentTimeMillis() - keepAliveTime;
            // If the keep-alive succeeded, ensure the session state is CONNECTED and schedule another keep-alive.
            if (error == null) {
              state.setState(PrimitiveState.CONNECTED);
              scheduleKeepAlive(System.currentTimeMillis(), delta);
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
   * Schedules a keep-alive request.
   */
  private synchronized void scheduleKeepAlive(long lastKeepAliveTime, long delta) {
    if (keepAliveTimer != null) {
      keepAliveTimer.cancel();
    }

    Duration delay = Duration.ofMillis(
        Math.max(Math.max((long) (timeout.toMillis() * TIMEOUT_FACTOR) - delta,
            timeout.toMillis() - MIN_TIMEOUT_DELTA - delta), 0));
    keepAliveTimer = getProxy().context().schedule(delay, () -> {
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
    return getProxy().closeSession(CloseSessionRequest.newBuilder()
        .setSessionId(state.getSessionId().id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return close().thenCompose(v -> getProxy().delete());
  }
}