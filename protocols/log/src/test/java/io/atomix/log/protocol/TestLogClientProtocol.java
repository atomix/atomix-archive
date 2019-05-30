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

import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;

/**
 * Test Raft client protocol.
 */
public class TestLogClientProtocol extends TestLogProtocol implements LogClientProtocol {
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
  public CompletableFuture<Void> consume(String memberId, ConsumeRequest request, StreamHandler<ConsumeResponse> handler) {
    return getServer(memberId).thenCompose(server -> server.consume(request, handler));
  }

  @Override
  public void reset(String memberId, ResetRequest request) {
    getServer(memberId).thenAccept(server -> server.reset(request));
  }
}
