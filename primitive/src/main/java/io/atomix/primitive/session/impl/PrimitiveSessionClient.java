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
package io.atomix.primitive.session.impl;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.protobuf.InvalidProtocolBufferException;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.CloseSessionRequest;
import io.atomix.primitive.service.OpenSessionRequest;
import io.atomix.primitive.service.OpenSessionResponse;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.ThreadContext;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive session client.
 */
public class PrimitiveSessionClient implements SessionClient {
  private final String name;
  private final PrimitiveType type;
  private final Duration timeout;
  private final PartitionClient client;
  private final ClusterMembershipService membershipService;
  private final ClusterCommunicationService communicationService;
  private final ThreadContext context;
  private final Map<Consumer<PrimitiveEvent>, Consumer<io.atomix.primitive.service.PrimitiveEvent>> eventListeners = new ConcurrentHashMap<>();
  private PrimitiveSessionState state;
  private PrimitiveSessionSequencer sequencer;
  private PrimitiveSessionInvoker invoker;
  private PrimitiveSessionListener listener;

  public PrimitiveSessionClient(
      String name,
      PrimitiveType type,
      Duration timeout,
      PartitionClient client,
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      ThreadContext context) {
    this.name = checkNotNull(name);
    this.type = checkNotNull(type);
    this.timeout = checkNotNull(timeout);
    this.client = checkNotNull(client);
    this.membershipService = checkNotNull(membershipService);
    this.communicationService = checkNotNull(communicationService);
    this.context = checkNotNull(context);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return type;
  }

  @Override
  public PrimitiveState getState() {
    return state.getState();
  }

  @Override
  public SessionId sessionId() {
    return state.getSessionId();
  }

  @Override
  public PartitionId partitionId() {
    return null;
  }

  @Override
  public ThreadContext context() {
    return context;
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    return invoker.invoke(operation);
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<io.atomix.primitive.service.PrimitiveEvent> wrappedListener = event ->
        listener.accept(new PrimitiveEvent(EventType.from(event.getType()), event.getValue().toByteArray()));
    eventListeners.put(listener, wrappedListener);
    this.listener.addEventListener(eventType.id(), wrappedListener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    Consumer<io.atomix.primitive.service.PrimitiveEvent> wrappedListener = eventListeners.remove(eventType.id());
    if (wrappedListener != null) {
      this.listener.removeEventListener(eventType.id(), wrappedListener);
    }
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    state.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    state.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<SessionClient> connect() {
    return client.write(OpenSessionRequest.newBuilder()
        .setMemberId(membershipService.getLocalMember().id().id())
        .setPrimitiveName(name)
        .setPrimitiveType(type.name())
        .setTimeout(timeout.toMillis())
        .build()
        .toByteArray())
        .thenApply(bytes -> {
          try {
            return OpenSessionResponse.parseFrom(bytes);
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
          }
        }).thenApply(response -> {
          state = new PrimitiveSessionState(
              SessionId.from(response.getSessionId()),
              name,
              type,
              response.getTimeout());
          sequencer = new PrimitiveSessionSequencer(state);
          invoker = new PrimitiveSessionInvoker(client, state, sequencer, context);
          listener = new PrimitiveSessionListener(membershipService, communicationService, state, sequencer, context);
          return this;
        });
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.write(CloseSessionRequest.newBuilder()
        .setSessionId(state.getSessionId().id())
        .build()
        .toByteArray())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return client.write(CloseSessionRequest.newBuilder()
        .setSessionId(state.getSessionId().id())
        .setDelete(true)
        .build()
        .toByteArray())
        .thenApply(response -> null);
  }
}
