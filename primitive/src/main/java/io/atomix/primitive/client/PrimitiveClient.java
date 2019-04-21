package io.atomix.primitive.client;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;

/**
 * Primitive client.
 */
public interface PrimitiveClient {

  <T extends Message, U extends Message> CompletableFuture<U> execute(
      OperationId operation, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder);

}
