package io.atomix.primitive.client;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.primitive.service.impl.ResponseContext;
import io.atomix.primitive.service.impl.StreamContext;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Primitive client.
 */
public interface PrimitiveClient {

  <T extends Message, U extends Message> CompletableFuture<Pair<ResponseContext, U>> execute(
      OperationId operation, RequestContext context, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder);

  <T extends Message, U extends Message> CompletableFuture<Void> execute(
      OperationId operation, RequestContext context, T request, ByteStringEncoder<T> encoder, StreamHandler<Pair<StreamContext, U>> handler, ByteBufferDecoder<U> decoder);

}
