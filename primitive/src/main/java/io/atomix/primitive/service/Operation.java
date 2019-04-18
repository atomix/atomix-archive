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
package io.atomix.primitive.service;

import java.util.function.Function;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * State machine operation.
 */
public abstract class Operation<T> {

  /**
   * Raft operation type.
   */
  public enum Type {
    COMMAND,
    QUERY,
  }

  private final long index;
  private final long timestamp;
  private final T value;

  protected Operation(long index, long timestamp, T value) {
    this.index = index;
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * Returns the operation type.
   *
   * @return the operation type
   */
  public abstract Type type();

  /**
   * Returns the operation index.
   *
   * @return the operation index
   */
  public long index() {
    return index;
  }

  /**
   * Returns the operation timestamp.
   *
   * @return the operation timestamp
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the operation value.
   *
   * @return the operation value
   */
  public T value() {
    return value;
  }

  /**
   * Returns a new instance of the operation with a new value type.
   *
   * @param mapper the mapper
   * @param <U>    the new value type
   * @return the new operation
   */
  public abstract <U> Operation<U> map(Function<T, U> mapper);

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("index", index())
        .add("timestamp", timestamp())
        .add("value", value instanceof byte[] ? ArraySizeHashPrinter.of((byte[]) value) : value)
        .toString();
  }

}
