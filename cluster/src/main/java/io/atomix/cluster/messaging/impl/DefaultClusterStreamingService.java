package io.atomix.cluster.messaging.impl;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.EncodingStreamHandler;
import io.atomix.utils.stream.StreamFunction;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.stream.TranscodingStreamFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default streaming service.
 */
@Component
public class DefaultClusterStreamingService implements ClusterStreamingService {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private static final Exception CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  @Dependency
  protected ClusterMembershipService membershipService;
  @Dependency
  protected MessagingService messagingService;

  @Override
  public <M> CompletableFuture<StreamHandler<M>> unicast(
      String type,
      Function<M, byte[]> encoder,
      MemberId memberId) {
    Member member = membershipService.getMember(memberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }
    return messagingService.sendStreamAsync(member.address(), type)
        .thenApply(stream -> new EncodingStreamHandler<>(stream, encoder));
  }

  @Override
  public <M, R> CompletableFuture<StreamFunction<M, CompletableFuture<R>>> send(
      String type,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      Duration timeout,
      MemberId memberId) {
    Member member = membershipService.getMember(memberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }
    return messagingService.sendStreamAndReceive(member.address(), type, timeout)
        .thenApply(function -> new TranscodingStreamFunction<>(function, encoder, f -> f.thenApply(decoder)));
  }

  @Override
  public <M, R> CompletableFuture<Void> send(
      String type,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      StreamHandler<R> handler,
      Duration timeout,
      MemberId memberId) {
    Member member = membershipService.getMember(memberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }
    return messagingService.sendAndReceiveStream(
        member.address(), type, encoder.apply(message), new EncodingStreamHandler<>(handler, decoder), timeout);
  }

  @Override
  public <M, R> CompletableFuture<StreamHandler<M>> send(
      String type,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      StreamHandler<R> handler,
      Duration timeout,
      MemberId memberId) {
    Member member = membershipService.getMember(memberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }
    return messagingService.sendStreamAndReceiveStream(
        member.address(), type, new EncodingStreamHandler<>(handler, decoder), timeout)
        .thenApply(h -> new EncodingStreamHandler<>(h, encoder));
  }

  @Override
  public <M, R> CompletableFuture<Void> subscribe(
      String type,
      Function<byte[], M> decoder,
      Supplier<StreamFunction<M, CompletableFuture<R>>> handler,
      Function<R, byte[]> encoder) {
    messagingService.registerStreamHandler(type, address ->
        new TranscodingStreamFunction<>(handler.get(), decoder, f -> f.thenApply(encoder)));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<Void> subscribe(
      String type,
      Function<byte[], M> decoder,
      BiConsumer<M, StreamHandler<R>> handler,
      Function<R, byte[]> encoder) {
    messagingService.registerStreamingHandler(type, (address, payload, stream) ->
        handler.accept(decoder.apply(payload), new EncodingStreamHandler<>(stream, encoder)));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<Void> subscribe(
      String type,
      Function<byte[], M> decoder,
      Function<StreamHandler<R>, StreamHandler<M>> handler,
      Function<R, byte[]> encoder) {
    messagingService.registerStreamingStreamHandler(type, (address, stream) ->
        new EncodingStreamHandler<>(handler.apply(new EncodingStreamHandler<>(stream, encoder)), decoder));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void unsubscribe(String type) {
    messagingService.unregisterHandler(type);
  }
}
