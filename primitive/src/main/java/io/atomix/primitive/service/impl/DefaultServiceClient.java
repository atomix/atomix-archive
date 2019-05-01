package io.atomix.primitive.service.impl;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import io.atomix.primitive.service.ServiceClient;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.impl.CommandRequest;
import io.atomix.primitive.service.impl.CommandResponse;
import io.atomix.primitive.service.impl.CreateRequest;
import io.atomix.primitive.service.impl.DeleteRequest;
import io.atomix.primitive.service.impl.QueryRequest;
import io.atomix.primitive.service.impl.QueryResponse;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.primitive.service.impl.ResponseContext;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.service.impl.ServiceRequest;
import io.atomix.primitive.service.impl.ServiceResponse;
import io.atomix.primitive.service.impl.StreamContext;
import io.atomix.primitive.service.impl.StreamResponse;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Default primitive client.
 */
public class DefaultServiceClient implements ServiceClient {
  private final ServiceId serviceId;
  private final PartitionClient client;

  public DefaultServiceClient(ServiceId serviceId, PartitionClient client) {
    this.serviceId = serviceId;
    this.client = client;
  }

  @Override
  public CompletableFuture<Void> create() {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setCreate(CreateRequest.newBuilder().build())
        .build()
        .toByteArray())
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setDelete(DeleteRequest.newBuilder().build())
        .build()
        .toByteArray())
        .thenApply(v -> null);
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Pair<ResponseContext, U>> execute(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder) {
    switch (operation.type()) {
      case COMMAND:
        return command(operation, context, request, encoder, decoder);
      case QUERY:
        return query(operation, context, request, encoder, decoder);
      default:
        return Futures.exceptionalFuture(new UnsupportedOperationException());
    }
  }

  private <T extends Message, U extends Message> CompletableFuture<Pair<ResponseContext, U>> command(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder) {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setCommand(CommandRequest.newBuilder()
            .setName(operation.id())
            .setCommand(ByteStringEncoder.encode(request, encoder))
            .setContext(context)
            .build()
            .toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getCommand().asReadOnlyByteBuffer(), CommandResponse::parseFrom))
        .thenApply(response -> Pair.of(response.getContext(), ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder)));
  }

  private <T extends Message, U extends Message> CompletableFuture<Pair<ResponseContext, U>> query(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      ByteBufferDecoder<U> decoder) {
    return client.query(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setQuery(QueryRequest.newBuilder()
            .setName(operation.id())
            .setQuery(ByteStringEncoder.encode(request, encoder))
            .setContext(context)
            .build()
            .toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getQuery().asReadOnlyByteBuffer(), QueryResponse::parseFrom))
        .thenApply(response -> Pair.of(response.getContext(), ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder)));
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Void> execute(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      StreamHandler<Pair<StreamContext, U>> handler,
      ByteBufferDecoder<U> decoder) {
    switch (operation.type()) {
      case COMMAND:
        return command(operation, context, request, encoder, handler, decoder);
      case QUERY:
        return query(operation, context, request, encoder, handler, decoder);
      default:
        return Futures.exceptionalFuture(new UnsupportedOperationException());
    }
  }

  private <T extends Message, U extends Message> CompletableFuture<Void> command(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      StreamHandler<Pair<StreamContext, U>> handler,
      ByteBufferDecoder<U> decoder) {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setCommand(CommandRequest.newBuilder()
            .setName(operation.id())
            .setCommand(ByteStringEncoder.encode(request, encoder))
            .setContext(context)
            .build()
            .toByteString())
        .build()
        .toByteArray(), new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        ServiceResponse serviceResponse = ByteArrayDecoder.decode(value, ServiceResponse::parseFrom);
        StreamResponse streamResponse = ByteBufferDecoder.decode(serviceResponse.getCommand().asReadOnlyByteBuffer(), StreamResponse::parseFrom);
        U response = ByteBufferDecoder.decode(streamResponse.getOutput().asReadOnlyByteBuffer(), decoder);
        handler.next(Pair.of(streamResponse.getContext(), response));
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

  private <T extends Message, U extends Message> CompletableFuture<Void> query(
      OperationId operation,
      RequestContext context,
      T request,
      ByteStringEncoder<T> encoder,
      StreamHandler<Pair<StreamContext, U>> handler,
      ByteBufferDecoder<U> decoder) {
    return client.query(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setQuery(QueryRequest.newBuilder()
            .setName(operation.id())
            .setQuery(ByteStringEncoder.encode(request, encoder))
            .setContext(context)
            .build()
            .toByteString())
        .build()
        .toByteArray(), new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        ServiceResponse serviceResponse = ByteArrayDecoder.decode(value, ServiceResponse::parseFrom);
        StreamResponse streamResponse = ByteBufferDecoder.decode(serviceResponse.getQuery().asReadOnlyByteBuffer(), StreamResponse::parseFrom);
        U response = ByteBufferDecoder.decode(streamResponse.getOutput().asReadOnlyByteBuffer(), decoder);
        handler.next(Pair.of(streamResponse.getContext(), response));
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
