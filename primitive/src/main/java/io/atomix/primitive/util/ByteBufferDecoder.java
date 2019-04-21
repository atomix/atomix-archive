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

import java.nio.ByteBuffer;

import io.atomix.primitive.PrimitiveException;

/**
 * Primitive operation decoder.
 */
@FunctionalInterface
public interface ByteBufferDecoder<T> {

  /**
   * Decodes the given bytes.
   *
   * @param bytes the bytes to decode
   * @param decoder the decoder with which to decode the bytes
   * @param <T> the object type
   * @return the decoded object
   */
  static <T> T decode(ByteBuffer bytes, ByteBufferDecoder<T> decoder) {
    try {
      return bytes != null ? decoder.decode(bytes) : null;
    } catch (Exception e) {
      throw new PrimitiveException.ServiceException(e);
    }
  }

  /**
   * Decodes the given bytes.
   *
   * @param bytes the bytes to decode
   * @return the decoded object
   * @throws Exception
   */
  T decode(ByteBuffer bytes) throws Exception;

}