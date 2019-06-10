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
package io.atomix.service.impl;

import io.atomix.service.util.ByteArrayDecoder;
import io.atomix.service.util.ByteArrayEncoder;

/**
 * Operation codec.
 *
 * @param <T> the request type
 * @param <U> the response type
 */
public class OperationCodec<T, U> {
  private final ByteArrayDecoder<T> decoder;
  private final ByteArrayEncoder<U> encoder;

  OperationCodec(ByteArrayDecoder<T> decoder, ByteArrayEncoder<U> encoder) {
    this.decoder = decoder != null ? decoder : v -> null;
    this.encoder = encoder != null ? encoder : v -> null;
  }

  /**
   * Decodes an operation request.
   *
   * @param bytes the request bytes
   * @return the decoded request
   */
  public T decode(byte[] bytes) {
    return ByteArrayDecoder.decode(bytes, decoder);
  }

  /**
   * Encodes an operation response.
   *
   * @param value the value to encode
   * @return the encoded response
   */
  public byte[] encode(U value) {
    return ByteArrayEncoder.encode(value, encoder);
  }
}