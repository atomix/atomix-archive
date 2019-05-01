package io.atomix.primitive.service.impl;

import java.util.HashMap;
import java.util.Map;

import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.StreamType;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;

/**
 * Service codec.
 */
public class ServiceCodec {
  private final Map<OperationId, OperationCodec> operations = new HashMap<>();
  private final Map<StreamType, StreamCodec> streams = new HashMap<>();

  public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId) {
    return register(operationId, new OperationCodec<>(null, null));
  }

  public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, ByteArrayDecoder<T> decoder) {
    return register(operationId, new OperationCodec<>(decoder, null));
  }

  public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, ByteArrayEncoder<U> encoder) {
    return register(operationId, new OperationCodec<>(null, encoder));
  }

  public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, ByteArrayDecoder<T> decoder, ByteArrayEncoder<U> encoder) {
    return register(operationId, new OperationCodec<>(decoder, encoder));
  }

  private <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, OperationCodec<T, U> operation) {
    operations.put(operationId, operation);
    return operation;
  }

  public <T> StreamCodec<T> register(StreamType<T> streamType, ByteArrayEncoder<T> encoder) {
    return register(streamType, new StreamCodec<>(encoder));
  }

  private <T> StreamCodec<T> register(StreamType<T> streamType, StreamCodec<T> codec) {
    streams.put(streamType, codec);
    return codec;
  }

  @SuppressWarnings("unchecked")
  public <T, U> OperationCodec<T, U> getOperation(OperationId<T, U> operationId) {
    return operations.get(operationId);
  }

  @SuppressWarnings("unchecked")
  public <T> StreamCodec<T> getStream(StreamType<T> streamType) {
    return streams.get(streamType);
  }

  @SuppressWarnings("unchecked")
  public <T> T decode(OperationId<T, ?> operationId, byte[] bytes) {
    return (T) operations.get(operationId).decode(bytes);
  }

  @SuppressWarnings("unchecked")
  public <U> byte[] encode(OperationId<?, U> operationId, U value) {
    return operations.get(operationId).encode(value);
  }

  @SuppressWarnings("unchecked")
  public <T> byte[] encode(StreamType<T> streamType, T value) {
    return streams.get(streamType).encode(value);
  }
}
