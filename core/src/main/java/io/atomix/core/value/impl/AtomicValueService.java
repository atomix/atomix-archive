/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.value.impl;

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;
import io.atomix.utils.time.Versioned;

/**
 * Atomic value service.
 */
public interface AtomicValueService {

  /**
   * Sets the value.
   *
   * @param value the value
   * @return the updated value
   */
  @Command
  Versioned<byte[]> set(byte[] value);

  /**
   * Gets the current value
   *
   * @return the current value
   */
  @Query
  Versioned<byte[]> get();

  /**
   * Updates the value if is matches the given expected value.
   *
   * @param expect the expected value
   * @param update the updated value
   * @return indicates whether the update was successful
   */
  @Command("cas")
  Versioned<byte[]> compareAndSet(byte[] expect, byte[] update);

  /**
   * Updates the value if is matches the given expected value.
   *
   * @param version the expected version
   * @param value the updated value
   * @return indicates whether the update was successful
   */
  @Command("casVersion")
  Versioned<byte[]> compareAndSet(long version, byte[] value);

  /**
   * Updates the value and returns the previous value.
   *
   * @param value the updated value
   * @return the previous value
   */
  @Command
  Versioned<byte[]> getAndSet(byte[] value);

  /**
   * Adds a listener to the service.
   */
  @Command
  void addListener();

  /**
   * Removes a listener from the service.
   */
  @Command
  void removeListener();

}
