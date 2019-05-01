package io.atomix.primitive.session.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.atomix.primitive.session.SessionStreamHandler;
import io.atomix.primitive.operation.StreamType;

/**
 * Primitive stream registry.
 */
public class PrimitiveStreamRegistry {
  private final Map<Long, PrimitiveSessionStream> idStreams = new HashMap<>();
  private final Map<String, Map<Long, PrimitiveSessionStream>> typeStreams = new HashMap<>();

  @SuppressWarnings("unchecked")
  public <T> void register(PrimitiveSessionStream<T> stream) {
    idStreams.put(stream.id().streamId(), stream);
    typeStreams.computeIfAbsent(stream.type().id(), type -> new HashMap<>()).put(stream.id().streamId(), stream);
  }

  public void unregister(SessionStreamHandler<?> stream) {
    idStreams.remove(stream.id().streamId());
    Map<Long, PrimitiveSessionStream> streams = this.typeStreams.get(stream.type().id());
    if (streams != null) {
      streams.remove(stream.id().streamId());
      if (streams.isEmpty()) {
        this.typeStreams.remove(stream.type().id());
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <T> PrimitiveSessionStream<T> getStream(long streamId) {
    return idStreams.get(streamId);
  }

  public Collection<PrimitiveSessionStream> getStreams() {
    return idStreams.values();
  }

  @SuppressWarnings("unchecked")
  public <T> Collection<PrimitiveSessionStream<T>> getStreams(StreamType<T> type) {
    Map<Long, PrimitiveSessionStream> streams = typeStreams.get(type.id());
    return streams != null ? (Collection) streams.values() : Collections.emptyList();
  }
}
