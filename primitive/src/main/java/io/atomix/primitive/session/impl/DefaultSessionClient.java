package io.atomix.primitive.session.impl;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.QueryId;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.service.impl.ServiceRequest;
import io.atomix.primitive.service.impl.ServiceResponse;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import io.atomix.utils.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Default session client.
 */
public class DefaultSessionClient implements SessionClient {
  private final ServiceId serviceId;
  private final PartitionClient client;

  public DefaultSessionClient(ServiceId serviceId, PartitionClient client) {
    this.serviceId = serviceId;
    this.client = client;
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    return command(SessionRequest.newBuilder()
        .setOpenSession(request)
        .build())
        .thenApply(response -> response.getOpenSession());
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    return command(SessionRequest.newBuilder()
        .setKeepAlive(request)
        .build())
        .thenApply(response -> response.getKeepAlive());
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    return command(SessionRequest.newBuilder()
        .setCloseSession(request)
        .build())
        .thenApply(response -> response.getCloseSession());
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Pair<SessionResponseContext, U>> execute(
      CommandId<T, U> command, SessionCommandContext context, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder) {
    return command(SessionRequest.newBuilder()
        .setCommand(SessionCommandRequest.newBuilder()
            .setContext(context)
            .setName(command.id())
            .setInput(ByteStringEncoder.encode(request, encoder))
            .build())
        .build())
        .thenApply(response -> response.getCommand())
        .thenApply(response -> Pair.of(response.getContext(), ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder)));
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Pair<SessionResponseContext, U>> execute(
      QueryId<T, U> query, SessionQueryContext context, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder) {
    return query(SessionRequest.newBuilder()
        .setQuery(SessionQueryRequest.newBuilder()
            .setContext(context)
            .setName(query.id())
            .setInput(ByteStringEncoder.encode(request, encoder))
            .build())
        .build())
        .thenApply(response -> response.getQuery())
        .thenApply(response -> Pair.of(response.getContext(), ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder)));
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Void> execute(
      CommandId<T, Void> command,
      SessionCommandContext context,
      T request,
      StreamHandler<Pair<SessionStreamContext, U>> handler,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder) {
    return command(SessionRequest.newBuilder()
            .setCommand(SessionCommandRequest.newBuilder()
                .setContext(context)
                .setName(command.id())
                .setInput(ByteStringEncoder.encode(request, encoder))
                .build())
            .build(),
        new StreamHandler<SessionResponse>() {
          @Override
          public void next(SessionResponse response) {
            handler.next(Pair.of(
                response.getStream().getContext(),
                ByteBufferDecoder.decode(response.getCommand().getOutput().asReadOnlyByteBuffer(), decoder)));
          }

          @Override
          public void complete() {
            handler.complete();
          }

          @Override
          public void error(Throwable error) {
            handler.error(error);
          }
        });
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Void> execute(
      QueryId<T, Void> query,
      SessionQueryContext context,
      T request,
      StreamHandler<Pair<SessionStreamContext, U>> handler,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder) {
    return query(SessionRequest.newBuilder()
            .setQuery(SessionQueryRequest.newBuilder()
                .setContext(context)
                .setName(query.id())
                .setInput(ByteStringEncoder.encode(request, encoder))
                .build())
            .build(),
        new StreamHandler<SessionResponse>() {
          @Override
          public void next(SessionResponse response) {
            handler.next(Pair.of(
                response.getStream().getContext(),
                ByteBufferDecoder.decode(response.getQuery().getOutput().asReadOnlyByteBuffer(), decoder)));
          }

          @Override
          public void complete() {
            handler.complete();
          }

          @Override
          public void error(Throwable error) {
            handler.error(error);
          }
        });
  }

  private CompletableFuture<SessionResponse> command(SessionRequest request) {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(request.toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getResponse().asReadOnlyByteBuffer(), SessionResponse::parseFrom));
  }

  private CompletableFuture<Void> command(SessionRequest request, StreamHandler<SessionResponse> handler) {
    return client.command(ServiceRequest.newBuilder()
            .setId(serviceId)
            .setRequest(request.toByteString())
            .build()
            .toByteArray(),
        new StreamHandler<byte[]>() {
          @Override
          public void next(byte[] response) {
            ServiceResponse serviceResponse = ByteArrayDecoder.decode(response, ServiceResponse::parseFrom);
            SessionResponse sessionResponse = ByteBufferDecoder.decode(serviceResponse.getResponse().asReadOnlyByteBuffer(), SessionResponse::parseFrom);
            handler.next(sessionResponse);
          }

          @Override
          public void complete() {
            handler.complete();
          }

          @Override
          public void error(Throwable error) {
            handler.error(error);
          }
        });
  }

  private CompletableFuture<SessionResponse> query(SessionRequest request) {
    return client.query(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(request.toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getResponse().asReadOnlyByteBuffer(), SessionResponse::parseFrom));
  }

  private CompletableFuture<Void> query(SessionRequest request, StreamHandler<SessionResponse> handler) {
    return client.query(ServiceRequest.newBuilder()
            .setId(serviceId)
            .setRequest(request.toByteString())
            .build()
            .toByteArray(),
        new StreamHandler<byte[]>() {
          @Override
          public void next(byte[] response) {
            ServiceResponse serviceResponse = ByteArrayDecoder.decode(response, ServiceResponse::parseFrom);
            SessionResponse sessionResponse = ByteBufferDecoder.decode(serviceResponse.getResponse().asReadOnlyByteBuffer(), SessionResponse::parseFrom);
            handler.next(sessionResponse);
          }

          @Override
          public void complete() {
            handler.complete();
          }

          @Override
          public void error(Throwable error) {
            handler.error(error);
          }
        });
  }
}
