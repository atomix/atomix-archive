package io.atomix.primitive.service;

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
 * Service client.
 */
public interface ServiceClient {

  /**
   * Executes an operation on the primitive.
   *
   * @param operation the operation to execute
   * @param context   the request context
   * @param request   the request
   * @param encoder   the request encoder
   * @param decoder   the response decoder
   * @param <T>       the request type
   * @param <U>       the response type
   * @return a future to be completed with the response
   */
  <T extends Message, U extends Message> CompletableFuture<Pair<ResponseContext, U>> execute(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder);

  /**
   * Executes a streaming operation on the primitive.
   *
   * @param operation the operation to execute
   * @param context   the request context
   * @param request   the request
   * @param encoder   the request encoder
   * @param handler   the response handler
   * @param decoder   the response decoder
   * @param <T>       the request type
   * @param <U>       the response type
   * @return a future to be completed once the operation has been completed
   */
  <T extends Message, U extends Message> CompletableFuture<Void> execute(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      StreamHandler<Pair<StreamContext, U>> handler,
      ByteBufferDecoder<U> decoder);

  /**
   * Creates the primitive.
   *
   * @return a future to be completed once the primitive has been created
   */
  CompletableFuture<Void> create();

  /**
   * Deletes the primitive.
   *
   * @return a future to be completed once the primitive has been deleted
   */
  CompletableFuture<Void> delete();

}
