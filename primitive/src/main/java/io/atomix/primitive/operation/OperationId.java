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
package io.atomix.primitive.operation;

import java.util.Objects;

/**
 * Operation ID.
 */
public abstract class OperationId<T, U> {
  private final String id;

  public OperationId(String id) {
    this.id = id;
  }

  /**
   * Returns the operation ID.
   *
   * @return the operation ID
   */
  public String id() {
    return id;
  }

  /**
   * Returns the operation type.
   *
   * @return the operation type
   */
  public abstract OperationType type();

  @Override
  public int hashCode() {
    return Objects.hash(id(), type());
  }

  @Override
  public boolean equals(Object object) {
    if (object.getClass() == getClass()) {
      OperationId that = (OperationId) object;
      return this.id.equals(that.id) && this.type().equals(that.type());
    }
    return false;
  }
}