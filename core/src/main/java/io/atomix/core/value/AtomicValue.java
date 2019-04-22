/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.core.value;

import java.util.Optional;

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.utils.time.Versioned;

/**
 * Distributed version of java.util.concurrent.atomic.AtomicReference.
 *
 * @param <V> value type
 */
public interface AtomicValue<V> extends SyncPrimitive {
  @Override
  default PrimitiveType type() {
    return AtomicValueType.instance();
  }

  /**
   * Atomically sets the value to the given updated value if the current value is equal to the expected value.
   * <p>
   * IMPORTANT: Equality is based on the equality of the serialized byte[] representations.
   * <p>
   *
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. false return indicates that the actual value was not equal to the expected value.
   */
  Optional<Versioned<V>> compareAndSet(V expect, V update);

  /**
   * Atomically sets the value to the given updated value if the current value is equal to the expected value.
   * <p>
   * IMPORTANT: Equality is based on the equality of the serialized byte[] representations.
   * <p>
   *
   * @param version the expected version
   * @param value the new value
   * @return true if successful. false return indicates that the actual value was not equal to the expected value.
   */
  Optional<Versioned<V>> compareAndSet(long version, V value);

  /**
   * Gets the current value.
   *
   * @return current value
   */
  Versioned<V> get();

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param value the new value
   * @return previous value
   */
  Versioned<V> getAndSet(V value);

  /**
   * Sets to the given value.
   *
   * @param value new value
   * @return the updated value
   */
  Versioned<V> set(V value);

  /**
   * Registers the specified listener to be notified whenever the atomic value is updated.
   *
   * @param listener listener to notify about events
   */
  void addListener(AtomicValueEventListener<V> listener);

  /**
   * Unregisters the specified listener such that it will no longer
   * receive atomic value update notifications.
   *
   * @param listener listener to unregister
   */
  void removeListener(AtomicValueEventListener<V> listener);

  @Override
  AsyncAtomicValue<V> async();
}
