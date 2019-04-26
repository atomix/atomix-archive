package io.atomix.cluster.messaging;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.cluster.MemberId;
import io.atomix.utils.stream.StreamFunction;
import io.atomix.utils.stream.StreamHandler;

/**
 * Cluster streaming service.
 */
public interface ClusterStreamingService {

  /**
   * Sends a message asynchronously to the specified communication address.
   * The message is specified using the type and payload.
   *
   * @param type     type of message.
   * @param memberId the member to which to send the message
   * @return future that is completed when the message is sent
   */
  <M> CompletableFuture<StreamHandler<M>> unicast(String type, Function<M, byte[]> encoder, MemberId memberId);

  default <M, R> CompletableFuture<StreamFunction<M, CompletableFuture<R>>> send(
      String type,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      MemberId memberId) {
    return send(type, encoder, decoder, (Duration) null, memberId);
  }

  <M, R> CompletableFuture<StreamFunction<M, CompletableFuture<R>>> send(
      String type,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      Duration timeout,
      MemberId memberId);

  default <M, R> CompletableFuture<Void> send(
      String type,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      StreamHandler<R> handler,
      MemberId memberId) {
    return send(type, message, encoder, decoder, handler, null, memberId);
  }

  <M, R> CompletableFuture<Void> send(
      String type,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      StreamHandler<R> handler,
      Duration timeout,
      MemberId memberId);

  default <M, R> CompletableFuture<StreamHandler<M>> send(
      String type,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      StreamHandler<R> handler,
      MemberId memberId) {
    return send(type, encoder, decoder, handler, null, memberId);
  }

  <M, R> CompletableFuture<StreamHandler<M>> send(
      String type,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      StreamHandler<R> handler,
      Duration timeout,
      MemberId memberId);

  <M, R> CompletableFuture<Void> subscribe(
      String type,
      Function<byte[], M> decoder,
      Supplier<StreamFunction<M, CompletableFuture<R>>> handler,
      Function<R, byte[]> encoder);

  <M, R> CompletableFuture<Void> subscribe(
      String type,
      Function<byte[], M> decoder,
      BiConsumer<M, StreamHandler<R>> handler,
      Function<R, byte[]> encoder);

  <M, R> CompletableFuture<Void> subscribe(
      String type,
      Function<byte[], M> decoder,
      Function<StreamHandler<R>, StreamHandler<M>> handler,
      Function<R, byte[]> encoder);

  void unsubscribe(String type);

}
