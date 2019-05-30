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
package io.atomix.client.lock;

import java.time.Duration;
import java.util.OptionalLong;

import io.atomix.client.PrimitiveType;
import io.atomix.client.SyncPrimitive;

/**
 * Asynchronous lock primitive.
 */
public interface AtomicLock extends SyncPrimitive {
  @Override
  default PrimitiveType type() {
    return AtomicLockType.instance();
  }

  /**
   * Acquires the lock, blocking until it's available.
   *
   * @return the acquired lock version
   */
  long lock();

  /**
   * Attempts to acquire the lock.
   *
   * @return indicates whether the lock was acquired
   */
  OptionalLong tryLock();

  /**
   * Attempts to acquire the lock for a specified amount of time.
   *
   * @param timeout the timeout after which to give up attempting to acquire the lock
   * @return indicates whether the lock was acquired
   */
  OptionalLong tryLock(Duration timeout);

  /**
   * Returns a boolean indicating whether the lock is locked.
   *
   * @return indicates whether the lock is locked
   */
  boolean isLocked();

  /**
   * Returns a boolean indicating whether the lock is locked with the given version.
   *
   * @param version the lock version
   * @return indicates whether the lock is locked
   */
  boolean isLocked(long version);

  /**
   * Unlocks the lock.
   */
  void unlock();

  /**
   * Unlocks the lock.
   *
   * @param version the lock version
   * @return indicates whether the lock was successfully unlocked
   */
  boolean unlock(long version);

  @Override
  AsyncAtomicLock async();

}
