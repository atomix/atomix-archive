/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

/**
 * Local client-side connection.
 */
final class LocalClientConnection extends AbstractClientConnection {
  private final LocalServerConnection serverConnection;

  LocalClientConnection(
      ScheduledExecutorService executorService,
      HandlerRegistry<String, BiConsumer<ProtocolMessage, ServerConnection>> handlers) {
    super(executorService);
    this.serverConnection = new LocalServerConnection(handlers, this);
  }

  @Override
  public CompletableFuture<Void> sendAsync(ProtocolMessage message) {
    serverConnection.dispatch(message);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<ProtocolReply> sendAndReceive(String subject, ProtocolMessage message, Duration timeout) {
    CompletableFuture<ProtocolReply> future = new CompletableFuture<>();
    new Callback(message.id(), subject, timeout, future);
    serverConnection.dispatch(message);
    return future;
  }

  @Override
  public void close() {
    super.close();
    serverConnection.close();
  }
}
