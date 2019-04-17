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
package io.atomix.raft.impl;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Operation result.
 */
public final class OperationResult {

  /**
   * Returns a successful operation result.
   *
   * @param result the operation result value
   * @return the operation result
   */
  public static OperationResult succeeded(byte[] result) {
    return new OperationResult(null, result);
  }

  /**
   * Returns a failed operation result.
   *
   * @param error the operation error
   * @return the operation result
   */
  public static OperationResult failed(Throwable error) {
    return new OperationResult(error, null);
  }

  private final Throwable error;
  private final byte[] result;

  private OperationResult(Throwable error, byte[] result) {
    this.error = error;
    this.result = result;
  }

  /**
   * Returns the operation error.
   *
   * @return the operation error
   */
  public Throwable error() {
    return error;
  }

  /**
   * Returns the result value.
   *
   * @return The result value.
   */
  public byte[] result() {
    return result;
  }

  /**
   * Returns whether the operation succeeded.
   *
   * @return whether the operation succeeded
   */
  public boolean succeeded() {
    return error == null;
  }

  /**
   * Returns whether the operation failed.
   *
   * @return whether the operation failed
   */
  public boolean failed() {
    return !succeeded();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("error", error)
        .add("result", ArraySizeHashPrinter.of(result))
        .toString();
  }
}
