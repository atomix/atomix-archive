package io.atomix.primitive.session;

import java.util.Objects;

/**
 * Stream ID.
 */
public class StreamId {
  private final SessionId sessionId;
  private final long streamId;

  public StreamId(SessionId sessionId, long streamId) {
    this.sessionId = sessionId;
    this.streamId = streamId;
  }

  /**
   * Returns the stream session ID.
   *
   * @return the stream session ID
   */
  public SessionId sessionId() {
    return sessionId;
  }

  /**
   * Returns the stream ID.
   *
   * @return the stream ID
   */
  public long streamId() {
    return streamId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sessionId, streamId);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof StreamId) {
      StreamId that = (StreamId) object;
      return this.sessionId.equals(that.sessionId) && this.streamId == that.streamId;
    }
    return false;
  }
}
