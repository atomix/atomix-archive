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
package io.atomix.primitive.util;

import io.atomix.primitive.PrimitiveException;

/**
 * Primitive operation encoder.
 */
@FunctionalInterface
public interface ByteArrayEncoder<T> {

  /**
   * Encodes the given object.
   *
   * @param object the object to encode
   * @param encoder the encoder with which to encode the object
   * @param <T> the object type
   * @return the decoded object
   */
  static <T> byte[] encode(T object, ByteArrayEncoder<T> encoder) {
    try {
      return object != null ? encoder.encode(object) : new byte[0];
    } catch (Exception e) {
      throw new PrimitiveException.ServiceException(e);
    }
  }

  /**
   * Encodes the given object.
   *
   * @param object the object to encode
   * @return the encoded object
   * @throws Exception
   */
  byte[] encode(T object) throws Exception;

}