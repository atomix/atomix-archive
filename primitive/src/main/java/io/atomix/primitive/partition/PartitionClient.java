/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.partition;

import java.util.concurrent.CompletableFuture;

import io.atomix.utils.stream.StreamHandler;

/**
 * Primitive client.
 */
public interface PartitionClient {

  /**
   * Sends a command to the partition.
   *
   * @param value the command to send
   * @return a future to be completed with the result
   */
  CompletableFuture<byte[]> command(byte[] value);

  /**
   * Sends a command to the partition.
   *
   * @param value the command to send
   * @return a future to be completed with the result
   */
  CompletableFuture<Void> command(byte[] value, StreamHandler<byte[]> handler);

  /**
   * Sets a query to the partition.
   *
   * @param value the query parameter
   * @return a future to be completed with the result
   */
  CompletableFuture<byte[]> query(byte[] value);

  /**
   * Sets a query to the partition.
   *
   * @param value the query parameter
   * @return a future to be completed with the result
   */
  CompletableFuture<Void> query(byte[] value, StreamHandler<byte[]> handler);

}
