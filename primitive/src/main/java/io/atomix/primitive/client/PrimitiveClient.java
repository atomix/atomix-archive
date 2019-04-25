package io.atomix.primitive.client;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import io.atomix.utils.StreamHandler;

/**
 * Primitive client.
 */
public interface PrimitiveClient {

  <T extends Message, U extends Message> CompletableFuture<U> execute(
      OperationId operation, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder);

  <T extends Message, U extends Message> CompletableFuture<Void> execute(
      OperationId operation, T request, ByteStringEncoder<T> encoder, StreamHandler<U> handler, ByteBufferDecoder<U> decoder);

}
