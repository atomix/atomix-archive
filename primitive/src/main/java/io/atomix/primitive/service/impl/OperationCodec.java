package io.atomix.primitive.service.impl;

import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;

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