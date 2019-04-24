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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Messaging handler registry.
 */
final class HandlerRegistry<T, U> {
  private final Map<T, U> handlers = new ConcurrentHashMap<>();

  /**
   * Registers a message type handler.
   *
   * @param id      the handler ID
   * @param handler the message handler
   */
  void register(T id, U handler) {
    handlers.put(id, handler);
  }

  /**
   * Unregisters a message type handler.
   *
   * @param id the handler ID
   */
  U unregister(T id) {
    return handlers.remove(id);
  }

  /**
   * Looks up a handler.
   *
   * @param type the message type
   * @return the handler or {@code null} if no handler of the given type is registered
   */
  U get(T type) {
    return handlers.get(type);
  }
}
