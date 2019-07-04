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
package io.atomix.node.service;

import io.atomix.utils.stream.StreamHandler;

/**
 * Operation executor.
 */
public interface OperationExecutor<T, U> {

  /**
   * Executes the operation.
   *
   * @param request the encoded request
   * @return the encoded response
   */
  byte[] execute(byte[] request);

  /**
   * Executes the operation.
   *
   * @param request the encoded request
   * @param handler the response handler
   */
  void execute(byte[] request, StreamHandler<byte[]> handler);

  /**
   * Executes the operation.
   *
   * @param request the request
   * @return the response
   */
  U execute(T request);

  /**
   * Executes the operation.
   *
   * @param request the request
   * @param handler the response handler
   */
  void execute(T request, StreamHandler<U> handler);

}
