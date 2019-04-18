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
package io.atomix.primitive;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.primitive.session.SessionClient;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Interface for primitive proxies.
 */
public interface PrimitiveProxy {

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  String name();

  /**
   * Returns the primitive type.
   *
   * @return the primitive type
   */
  PrimitiveType type();

  /**
   * Returns the proxy thread context.
   *
   * @return the proxy thread context
   */
  ThreadContext context();

  /**
   * Connects the primitive.
   *
   * @return a future to be completed once the primitive has been connected
   */
  CompletableFuture<Void> connect();

  /**
   * Closes the primitive.
   *
   * @return a future to be completed once the primitive has been closed
   */
  CompletableFuture<Void> close();

  /**
   * Deletes the primitive.
   *
   * @return a future to be completed once the primitive has been deleted
   */
  CompletableFuture<Void> delete();

  /**
   * Returns the current primitive state.
   *
   * @return the current primitive state
   */
  PrimitiveState getState();

  /**
   * Adds a state change listener to the proxy.
   *
   * @param listener the listener to add
   */
  void onStateChange(Consumer<PrimitiveState> listener);
}