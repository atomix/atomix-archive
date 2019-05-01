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
import java.util.function.Consumer;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.ManagedAsyncPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;

/**
 * Session enabled asynchronous primitive.
 */
public abstract class SessionEnabledAsyncPrimitive<P extends SessionEnabledPrimitiveProxy, T extends AsyncPrimitive>
    extends SimpleAsyncPrimitive<P>
    implements ManagedAsyncPrimitive<T> {
  private final Duration timeout;
  private final PrimitiveManagementService managementService;
  private PrimitiveSessionState state;
  private PrimitiveSessionSequencer sequencer;
  private PrimitiveSessionExecutor<P> executor;

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
              return (T) this;
            }));
  }

  @Override
  public CompletableFuture<Void> close() {
    return getProxy().closeSession(CloseSessionRequest.newBuilder()
        .setSessionId(state.getSessionId().id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }
}