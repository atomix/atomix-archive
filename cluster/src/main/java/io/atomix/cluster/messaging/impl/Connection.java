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

import io.atomix.utils.stream.StreamHandler;

interface Connection {

  /**
   * Dispatches a message received on the connection.
   *
   * @param message the message to dispatch
   */
  void dispatch(ProtocolMessage message);

  /**
   * Registers a stream handler.
   *
   * @param id      the stream ID
   * @param handler the stream handler
   */
  void registerStreamHandler(long id, StreamHandler<byte[]> handler);

  /**
   * Unregisters a stream handler.
   *
   * @param id the stream ID
   * @return the unregistered handler
   */
  StreamHandler<byte[]> unregisterStreamHandler(long id);

  /**
   * Closes the connection.
   */
  default void close() {
  }
}
