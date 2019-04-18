/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.session;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.OperationDecoder;
import io.atomix.primitive.operation.OperationEncoder;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationMetadata;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Partition proxy.
 */
public interface SessionClient {

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  String name();

  /**
   * Returns the client proxy type.
   *
   * @return The client proxy type.
   */
  PrimitiveType type();

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  PrimitiveState getState();

  /**
   * Returns the proxy session identifier.
   *
   * @return The proxy session identifier
   */
  SessionId sessionId();

  /**
   * Returns the partition identifier.
   *
   * @return the partition identifier.
   */
  PartitionId partitionId();

  /**
   * Returns the partition thread context.
   *
   * @return the partition thread context
   */
  ThreadContext context();

  /**
   * Executes an operation to the cluster.
   *
   * @param operation the operation to execute
   * @return a future to be completed with the operation result
   * @throws NullPointerException if {@code operation} is null
   */
  CompletableFuture<byte[]> execute(PrimitiveOperation operation);

  /**
   * Executes an operation.
   *
   * @param operationId the operation ID
   * @param operation   the operation value
   * @param encoder     the operation encoder
   * @param decoder     the operation decoder
   * @param <T>         the operation input type
   * @param <U>         the operation output type
   * @return the operation result
   */
  default <T, U> CompletableFuture<U> execute(
      OperationId operationId, T operation, OperationEncoder<T> encoder, OperationDecoder<U> decoder) {
    return OrderedFuture.wrap(CompletableFuture.completedFuture(operation))
        .thenApply(object -> OperationEncoder.encode(object, encoder))
        .thenCompose(bytes -> execute(PrimitiveOperation.newBuilder()
            .setMetadata(OperationMetadata.newBuilder()
                .setName(operationId.id())
                .setType(operationId.type())
                .build())
            .setValue(ByteString.copyFrom(bytes))
            .build()))
        .thenApply(bytes -> OperationDecoder.decode(bytes, decoder));
  }

  /**
   * Adds an event listener.
   *
   * @param eventType the event type for which to add the listener
   * @param listener  the event listener to add
   * @param decoder the event decoder
   */
  default <T> void addEventListener(EventType eventType, Consumer<T> listener, OperationDecoder<T> decoder) {
    // TODO: This listener is not removable
    addEventListener(eventType, event -> listener.accept(OperationDecoder.decode(event.value(), decoder)));
  }

  /**
   * Adds an event listener.
   *
   * @param eventType the event type for which to add the listener
   * @param listener  the event listener to add
   */
  void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener);

  /**
   * Removes an event listener.
   *
   * @param eventType the event type for which to remove the listener
   * @param listener  the event listener to remove
   */
  void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener);

  /**
   * Registers a session state change listener.
   *
   * @param listener The callback to call when the session state changes.
   */
  void addStateChangeListener(Consumer<PrimitiveState> listener);

  /**
   * Removes a state change listener.
   *
   * @param listener the state change listener to remove
   */
  void removeStateChangeListener(Consumer<PrimitiveState> listener);

  /**
   * Connects the proxy.
   *
   * @return a future to be completed once the proxy has been connected
   */
  CompletableFuture<SessionClient> connect();

  /**
   * Closes the proxy.
   *
   * @return a future to be completed once the proxy has been closed
   */
  CompletableFuture<Void> close();

  /**
   * Closes the session and deletes the service.
   *
   * @return a future to be completed once the service has been deleted
   */
  CompletableFuture<Void> delete();

  /**
   * Partition proxy builder.
   */
  abstract class Builder implements io.atomix.utils.Builder<SessionClient> {
  }
}
