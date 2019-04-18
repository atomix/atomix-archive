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
package io.atomix.log.protocol;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import io.atomix.utils.concurrent.Futures;

/**
 * Test Raft client protocol.
 */
public class TestLogClientProtocol extends TestLogProtocol implements LogClientProtocol {
  private final Map<Long, Consumer<RecordsRequest>> consumers = new ConcurrentHashMap<>();

  public TestLogClientProtocol(String memberId, Map<String, TestLogServerProtocol> servers, Map<String, TestLogClientProtocol> clients) {
    super(servers, clients);
    clients.put(memberId, this);
  }

  private CompletableFuture<TestLogServerProtocol> getServer(String memberId) {
    TestLogServerProtocol server = server(memberId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(String memberId, AppendRequest request) {
    return getServer(memberId).thenCompose(server -> server.append(request));
  }

  @Override
  public CompletableFuture<ConsumeResponse> consume(String memberId, ConsumeRequest request) {
    return getServer(memberId).thenCompose(server -> server.consume(request));
  }

  @Override
  public void reset(String memberId, ResetRequest request) {
    getServer(memberId).thenAccept(server -> server.reset(request));
  }

  @Override
  public void registerRecordsConsumer(long consumerId, Consumer<RecordsRequest> handler, Executor executor) {
    consumers.put(consumerId, request -> executor.execute(() -> handler.accept(request)));
  }

  @Override
  public void unregisterRecordsConsumer(long consumerId) {
    consumers.remove(consumerId);
  }

  void produce(long consumerId, RecordsRequest request) {
    Consumer<RecordsRequest> consumer = consumers.get(consumerId);
    if (consumer != null) {
      consumer.accept(request);
    }
  }
}
