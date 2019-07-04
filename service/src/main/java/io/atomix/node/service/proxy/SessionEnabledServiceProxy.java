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
package io.atomix.node.service.proxy;

import java.util.concurrent.CompletableFuture;

import io.atomix.node.service.client.SessionClient;
import io.atomix.node.service.protocol.CloseSessionRequest;
import io.atomix.node.service.protocol.CloseSessionResponse;
import io.atomix.node.service.protocol.KeepAliveRequest;
import io.atomix.node.service.protocol.KeepAliveResponse;
import io.atomix.node.service.protocol.OpenSessionRequest;
import io.atomix.node.service.protocol.OpenSessionResponse;

/**
 * Interface for session aware primitive proxies.
 */
public abstract class SessionEnabledServiceProxy extends AbstractServiceProxy<SessionClient> {
  public SessionEnabledServiceProxy(SessionClient client) {
    super(client);
  }

  @Override
  public String name() {
    return getClient().name();
  }

  @Override
  public String type() {
    return getClient().type();
  }

  /**
   * Opens a primitive session.
   *
   * @param request the open session request
   * @return a future to be completed with the open session response
   */
  public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    return getClient().openSession(request);
  }

  /**
   * Keeps a session alive.
   *
   * @param request the keep alive request
   * @return a future to be completed with the keep-alive response
   */
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    return getClient().keepAlive(request);
  }

  /**
   * Closes a session.
   *
   * @param request the close session request
   * @return a future to be completed with the close session response
   */
  public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    return getClient().closeSession(request);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return getClient().delete();
  }
}