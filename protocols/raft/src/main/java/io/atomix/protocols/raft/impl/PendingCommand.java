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
package io.atomix.protocols.raft.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.protocols.raft.protocol.OperationRequest;
import io.atomix.protocols.raft.protocol.OperationResponse;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Pending command.
 */
public final class PendingCommand {
  private final OperationRequest request;
  private final CompletableFuture<OperationResponse> future;

  public PendingCommand(OperationRequest request, CompletableFuture<OperationResponse> future) {
    this.request = request;
    this.future = future;
  }

  /**
   * Returns the pending command request.
   *
   * @return the pending command request
   */
  public OperationRequest request() {
    return request;
  }

  /**
   * Returns the pending command future.
   *
   * @return the pending command future
   */
  public CompletableFuture<OperationResponse> future() {
    return future;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("request", request)
        .toString();
  }
}
