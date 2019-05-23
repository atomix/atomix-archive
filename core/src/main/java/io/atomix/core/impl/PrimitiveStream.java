package io.atomix.core.impl;

import io.atomix.primitive.headers.SessionStreamHeader;

/**
 * Primitive stream.
 */
public class PrimitiveStream {
  private final long streamId;
  private long index;
  private long lastItemNumber;

  public PrimitiveStream(long streamId) {
    this.streamId = streamId;
  }

  /**
   * Returns the stream header.
   *
   * @return the stream header
   */
  public SessionStreamHeader getHeader() {
    return SessionStreamHeader.newBuilder()
        .setStreamId(streamId)
        .setIndex(index)
        .setLastItemNumber(lastItemNumber)
        .build();
  }

  /**
   * Updates the stream state with the given response header.
   *
   * @param header the stream header
   */
  void update(SessionStreamHeader header) {
    index = header.getIndex();
    lastItemNumber = header.getLastItemNumber();
  }
}
