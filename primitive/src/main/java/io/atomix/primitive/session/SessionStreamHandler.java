package io.atomix.primitive.session;

import io.atomix.primitive.operation.StreamType;
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

  /**
   * Returns the stream type.
   *
   * @return the stream type
   */
  StreamType<T> type();

}
