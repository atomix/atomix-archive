package io.atomix.cluster.messaging.impl;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.ManagedClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.utils.StreamFunction;
import io.atomix.utils.StreamHandler;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default streaming service.
 */
public class DefaultClusterStreamingService implements ManagedClusterStreamingService {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private static final Exception CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  protected final ClusterMembershipService membershipService;
  protected final MessagingService messagingService;
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultClusterStreamingService(ClusterMembershipService membershipService, MessagingService messagingService) {
    this.membershipService = membershipService;
    this.messagingService = messagingService;
  }

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
        .thenApply(stream -> new TranscodingStreamHandler<>(stream, encoder));
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
        member.address(), type, encoder.apply(message), new TranscodingStreamHandler<>(handler, decoder), timeout);
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
        member.address(), type, new TranscodingStreamHandler<>(handler, decoder), timeout)
        .thenApply(h -> new TranscodingStreamHandler<>(h, encoder));
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
        handler.accept(decoder.apply(payload), new TranscodingStreamHandler<>(stream, encoder)));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<Void> subscribe(
      String type,
      Function<byte[], M> decoder,
      Function<StreamHandler<R>, StreamHandler<M>> handler,
      Function<R, byte[]> encoder) {
    messagingService.registerStreamingStreamHandler(type, (address, stream) ->
        new TranscodingStreamHandler<>(handler.apply(new TranscodingStreamHandler<>(stream, encoder)), decoder));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void unsubscribe(String type) {
    messagingService.unregisterHandler(type);
  }

  @Override
  public CompletableFuture<ClusterStreamingService> start() {
    if (started.compareAndSet(false, true)) {
      log.info("Started");
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      log.info("Stopped");
    }
    return CompletableFuture.completedFuture(null);
  }
}
