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
package io.atomix.raft.protocol;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Test Raft client protocol.
 */
public class TestRaftClientProtocol extends TestRaftProtocol implements RaftClientProtocol {
  public TestRaftClientProtocol(Map<String, TestRaftServerProtocol> servers,
      ThreadContext context) {
    super(servers, context);
  }

  private CompletableFuture<TestRaftServerProtocol> getServer(String serverId) {
    TestRaftServerProtocol server = server(serverId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(String server, QueryRequest request) {
    return scheduleTimeout(getServer(server).thenCompose(protocol -> protocol.query(request)));
  }

  @Override
  public CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler) {
    return getServer(server).thenCompose(protocol -> protocol.queryStream(request, handler));
  }

  @Override
  public CompletableFuture<CommandResponse> command(String server, CommandRequest request) {
    return scheduleTimeout(getServer(server).thenCompose(protocol -> protocol.command(request)));
  }

  @Override
  public CompletableFuture<Void> commandStream(String server, CommandRequest request, StreamHandler<CommandResponse> handler) {
    return getServer(server).thenCompose(protocol -> protocol.commandStream(request, handler));
  }
}
