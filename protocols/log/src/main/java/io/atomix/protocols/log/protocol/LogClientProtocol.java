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
package io.atomix.protocols.log.protocol;

import java.util.concurrent.CompletableFuture;

import io.atomix.utils.stream.StreamHandler;

/**
 * Primary-backup protocol client.
 */
public interface LogClientProtocol {

  /**
   * Sends an append request to the given node.
   *
   * @param memberId the node to which to send the request
   * @param request  the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<AppendResponse> append(String memberId, AppendRequest request);

  /**
   * Sends a consume request to the given node.
   *
   * @param memberId the node to which to send the request
   * @param request  the request to send
   * @param handler  the stream handler
   * @return a future to be completed with the response
   */
  CompletableFuture<Void> consume(String memberId, ConsumeRequest request, StreamHandler<ConsumeResponse> handler);

  /**
   * Sends a reset request to the given node.
   *
   * @param memberId the node to which to send the request
   * @param request  the request to send
   */
  void reset(String memberId, ResetRequest request);

}
