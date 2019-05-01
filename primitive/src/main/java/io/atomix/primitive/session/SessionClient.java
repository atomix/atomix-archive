package io.atomix.primitive.session;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.QueryId;
import io.atomix.primitive.operation.StreamType;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.CloseSessionResponse;
import io.atomix.primitive.session.impl.KeepAliveRequest;
import io.atomix.primitive.session.impl.KeepAliveResponse;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.OpenSessionResponse;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionResponseContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Session client.
 */
public interface SessionClient {

  /**
   * Executes a command on the service.
   *
   * @param command the command to execute
   * @param context the command context
   * @param request the request
   * @param encoder the request encoder
   * @param decoder the response decoder
   * @param <T>     the request type
   * @param <U>     the response type
   * @return a future to be completed with the response
   */
  <T extends Message, U extends Message> CompletableFuture<Pair<SessionResponseContext, U>> execute(
      CommandId<T, U> command,
      SessionCommandContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder);

  /**
   * Executes a query on the service.
   *
   * @param query   the query to execute
   * @param context the query context
   * @param request the request
   * @param encoder the request encoder
   * @param decoder the response decoder
   * @param <T>     the request type
   * @param <U>     the response type
   * @return a future to be completed with the response
   */
  <T extends Message, U extends Message> CompletableFuture<Pair<SessionResponseContext, U>> execute(
      QueryId<T, U> query,
      SessionQueryContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder);

  /**
   * Executes an asynchronous command on the service.
   *
   * @param command    the command to execute
   * @param streamType the stream type
   * @param context    the command context
   * @param request    the request
   * @param encoder    the request encoder
   * @param decoder    the response decoder
   * @param <T>        the request type
   * @param <U>        the response type
   * @return a future to be completed with the response
   */
  <T extends Message, U extends Message> CompletableFuture<Pair<SessionResponseContext, U>> execute(
      CommandId<T, U> command,
      StreamType<U> streamType,
      SessionCommandContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder);

  /**
   * Executes an asynchronous query on the service.
   *
   * @param query      the query to execute
   * @param streamType the stream type
   * @param context    the query context
   * @param request    the request
   * @param encoder    the request encoder
   * @param decoder    the response decoder
   * @param <T>        the request type
   * @param <U>        the response type
   * @return a future to be completed with the response
   */
  <T extends Message, U extends Message> CompletableFuture<Pair<SessionResponseContext, U>> execute(
      QueryId<T, U> query,
      StreamType<U> streamType,
      SessionQueryContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder);

  /**
   * Executes a streaming command on the service.
   *
   * @param command    the command to execute
   * @param streamType the stream type
   * @param context    the command context
   * @param request    the request
   * @param handler    the response handler
   * @param encoder    the request encoder
   * @param decoder    the response decoder
   * @param <T>        the request type
   * @param <U>        the response type
   * @return a future to be completed with the response
   */
  <T extends Message, U extends Message> CompletableFuture<SessionResponseContext> execute(
      CommandId<T, U> command,
      StreamType<U> streamType,
      SessionCommandContext context,
      T request,
      StreamHandler<Pair<SessionStreamContext, U>> handler,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder);

  /**
   * Executes a streaming query on the service.
   *
   * @param query      the query to execute
   * @param streamType the stream type
   * @param context    the query context
   * @param request    the request
   * @param handler    the response handler
   * @param encoder    the request encoder
   * @param decoder    the response decoder
   * @param <T>        the request type
   * @param <U>        the response type
   * @return a future to be completed with the response
   */
  <T extends Message, U extends Message> CompletableFuture<SessionResponseContext> execute(
      QueryId<T, U> query,
      StreamType<U> streamType,
      SessionQueryContext context,
      T request,
      StreamHandler<Pair<SessionStreamContext, U>> handler,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder);

  /**
   * Opens a session.
   *
   * @param request the open session request
   * @return a future to be completed with the open session response
   */
  CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request);

  /**
   * Sends a session keep-alive.
   *
   * @param request the keep-alive request
   * @return a future to be completed with the keep-alive response
   */
  CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

  /**
   * Closes a session.
   *
   * @param request the close session request
   * @return a future to be completed with the close session response
   */
  CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request);

  /**
   * Deletes the service.
   *
   * @return a future to be completed once the service has been deleted
   */
  CompletableFuture<Void> delete();

}
