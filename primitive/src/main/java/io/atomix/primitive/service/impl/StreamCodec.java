package io.atomix.primitive.service.impl;

import io.atomix.primitive.util.ByteArrayEncoder;

/**
 * Stream codec.
 *
 * @param <T> the stream type
 */
public class StreamCodec<T> {
  private final ByteArrayEncoder<T> encoder;

  StreamCodec(ByteArrayEncoder<T> encoder) {
    this.encoder = encoder != null ? encoder : v -> null;
  }

  /**
   * Encodes the given value for the stream.
   *
   * @param value the value to encode
   * @return the encoded value
   */
  public byte[] encode(T value) {
    return ByteArrayEncoder.encode(value, encoder);
  }
}