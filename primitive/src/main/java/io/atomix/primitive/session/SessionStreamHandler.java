package io.atomix.primitive.session;

import io.atomix.utils.stream.StreamHandler;

/**
 * Session stream handler.
 */
public interface SessionStreamHandler<T> extends StreamHandler<T> {

  /**
   * Returns the stream ID.
   *
   * @return the stream ID
   */
  StreamId id();

}
