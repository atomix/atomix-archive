package io.atomix.primitive.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.Message;
import io.atomix.primitive.client.PrimitiveClient;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.impl.CommandRequest;
import io.atomix.primitive.service.impl.CommandResponse;
import io.atomix.primitive.service.impl.QueryRequest;
import io.atomix.primitive.service.impl.QueryResponse;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.service.impl.ServiceRequest;
import io.atomix.primitive.service.impl.ServiceResponse;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.Futures;

/**
 * Default primitive client.
 */
public class DefaultPrimitiveClient implements PrimitiveClient {
  private final ServiceId serviceId;
  private final PartitionClient client;
  private final AtomicLong lastIndex = new AtomicLong();

  public DefaultPrimitiveClient(ServiceId serviceId, PartitionClient client) {
    this.serviceId = serviceId;
    this.client = client;
  }

  protected CommandResponse recordIndex(CommandResponse response) {
    lastIndex.accumulateAndGet(response.getIndex(), Math::max);
    return response;
  }

  protected long getIndex() {
    return lastIndex.get();
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<U> execute(OperationId operation, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder) {
    switch (operation.type()) {
      case COMMAND:
        return command(operation, request, encoder, decoder);
      case QUERY:
        return query(operation, request, encoder, decoder);
      default:
        return Futures.exceptionalFuture(new UnsupportedOperationException());
    }
  }

  private <T extends Message, U extends Message> CompletableFuture<U> command(OperationId operation, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder) {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(CommandRequest.newBuilder()
            .setName(operation.id())
            .setCommand(ByteStringEncoder.encode(request, encoder))
            .build()
            .toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getResponse().asReadOnlyByteBuffer(), CommandResponse::parseFrom))
        .thenApply(this::recordIndex)
        .thenApply(response -> ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder));
  }

  private <T extends Message, U extends Message> CompletableFuture<U> query(OperationId operation, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder) {
    return client.query(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(QueryRequest.newBuilder()
            .setName(operation.id())
            .setQuery(ByteStringEncoder.encode(request, encoder))
            .setIndex(lastIndex.get())
            .build()
            .toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getResponse().asReadOnlyByteBuffer(), QueryResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder));
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Void> execute(
      OperationId operation, T request, ByteStringEncoder<T> encoder, StreamHandler<U> handler, ByteBufferDecoder<U> decoder) {
    switch (operation.type()) {
      case COMMAND:
        return command(operation, request, encoder, handler, decoder);
      case QUERY:
        return query(operation, request, encoder, handler, decoder);
      default:
        return Futures.exceptionalFuture(new UnsupportedOperationException());
    }
  }

  private <T extends Message, U extends Message> CompletableFuture<Void> command(
      OperationId operation, T request, ByteStringEncoder<T> encoder, StreamHandler<U> handler, ByteBufferDecoder<U> decoder) {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(CommandRequest.newBuilder()
            .setName(operation.id())
            .setCommand(ByteStringEncoder.encode(request, encoder))
            .build()
            .toByteString())
        .build()
        .toByteArray(), new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        ServiceResponse serviceResponse = ByteArrayDecoder.decode(value, ServiceResponse::parseFrom);
        CommandResponse commandResponse = ByteBufferDecoder.decode(serviceResponse.getResponse().asReadOnlyByteBuffer(), CommandResponse::parseFrom);
        U response = ByteBufferDecoder.decode(commandResponse.getOutput().asReadOnlyByteBuffer(), decoder);
        handler.next(response);
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
      OperationId operation, T request, ByteStringEncoder<T> encoder, StreamHandler<U> handler, ByteBufferDecoder<U> decoder) {
    return client.query(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(QueryRequest.newBuilder()
            .setName(operation.id())
            .setQuery(ByteStringEncoder.encode(request, encoder))
            .setIndex(lastIndex.get())
            .build()
            .toByteString())
        .build()
        .toByteArray(), new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        ServiceResponse serviceResponse = ByteArrayDecoder.decode(value, ServiceResponse::parseFrom);
        QueryResponse queryResponse = ByteBufferDecoder.decode(serviceResponse.getResponse().asReadOnlyByteBuffer(), QueryResponse::parseFrom);
        U response = ByteBufferDecoder.decode(queryResponse.getOutput().asReadOnlyByteBuffer(), decoder);
        handler.next(response);
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
