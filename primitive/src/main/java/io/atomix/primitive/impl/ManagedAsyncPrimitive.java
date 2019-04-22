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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.EventContext;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionContext;
import io.atomix.primitive.session.impl.SessionMetadata;
import io.atomix.utils.concurrent.Futures;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Session enabled asynchronous primitive.
 */
public abstract class ManagedAsyncPrimitive<P extends SessionEnabledPrimitiveProxy> extends SimpleAsyncPrimitive<P> {
  private final Duration timeout;
  private final PrimitiveManagementService managementService;
  private PrimitiveSessionState state;
  private PrimitiveSessionSequencer sequencer;
  private PrimitiveSessionInvoker<P> invoker;
  private PrimitiveSessionListener<P> listener;

  public ManagedAsyncPrimitive(
      P proxy,
      Duration timeout,
      PrimitiveManagementService managementService) {
    super(proxy);
    this.timeout = timeout;
    this.managementService = managementService;
  }

  protected <T> CompletableFuture<T> command(BiFunction<P, SessionMetadata, CompletableFuture<Pair<SessionContext, T>>> function) {
    return invoker.command(function);
  }

  protected <T> CompletableFuture<T> query(BiFunction<P, SessionMetadata, CompletableFuture<Pair<SessionContext, T>>> function) {
    return invoker.query(function);
  }

  protected void event(BiConsumer<P, SessionMetadata> consumer) {
    listener.event(consumer);
  }

  protected <T> BiConsumer<EventContext, T> listener(Consumer<T> consumer) {
    return listener.listener(consumer);
  }

  protected void state(Consumer<PrimitiveState> consumer) {
    state.addStateChangeListener(consumer);
  }

  protected PrimitiveState getState() {
    return state.getState();
  }

  /**
   * Connects the primitive.
   *
   * @return a future to be completed once the primitive has been connected
   */
  public CompletableFuture<Void> connect() {
    return managementService.getSessionIdService().nextSessionId()
        .thenCompose(sessionId -> getProxy().openSession(OpenSessionRequest.newBuilder()
            .setMemberId(managementService.getMembershipService().getLocalMember().id().id())
            .setSessionId(sessionId.id())
            .setTimeout(timeout.toMillis())
            .build()).thenApply(response -> {
          ManagedPrimitiveContext context = new ManagedPrimitiveContext(
              managementService.getMembershipService().getLocalMember().id(),
              sessionId,
              name(),
              type(),
              timeout);
          state = new PrimitiveSessionState(sessionId, timeout.toMillis());
          sequencer = new PrimitiveSessionSequencer(state, context);
          invoker = new PrimitiveSessionInvoker<>(getProxy(), state, context, sequencer);
          listener = new PrimitiveSessionListener<>(getProxy(), state, sequencer);
          return null;
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