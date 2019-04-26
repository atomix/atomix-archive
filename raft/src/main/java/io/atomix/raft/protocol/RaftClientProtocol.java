/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.raft.protocol;

import java.util.concurrent.CompletableFuture;

import io.atomix.utils.stream.StreamHandler;

/**
 * Raft client protocol.
 */
public interface RaftClientProtocol {

  /**
   * Sends a query request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<QueryResponse> query(String server, QueryRequest request);

  /**
   * Sends a query request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler);

  /**
   * Sends a command request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CommandResponse> command(String server, CommandRequest request);

  /**
   * Sends a command request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<Void> commandStream(String server, CommandRequest request, StreamHandler<CommandResponse> handler);

}