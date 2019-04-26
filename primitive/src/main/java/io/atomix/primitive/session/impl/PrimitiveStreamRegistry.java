package io.atomix.primitive.session.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Primitive stream registry.
 */
public class PrimitiveStreamRegistry {
  private final Map<Long, PrimitiveSessionStream> streams = new HashMap<>();

  public void register(PrimitiveSessionStream stream) {
    streams.put(stream.id().streamId(), stream);
  }

  public void unregister(PrimitiveSessionStream stream) {
    streams.remove(stream.id().streamId());
  }

  @SuppressWarnings("unchecked")
  public <T> PrimitiveSessionStream<T> getStream(long streamId) {
    return streams.get(streamId);
  }

  public Collection<PrimitiveSessionStream> getStreams() {
    return streams.values();
  }
}
