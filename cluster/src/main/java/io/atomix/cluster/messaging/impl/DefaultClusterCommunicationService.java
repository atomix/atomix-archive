/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import java.net.ConnectException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;

/**
 * Cluster communication service implementation.
 */
@Component
public class DefaultClusterCommunicationService implements ClusterCommunicationService {
  private static final Exception CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  @Dependency
  protected ClusterMembershipService membershipService;
  @Dependency
  protected MessagingService messagingService;

  private MemberId getMemberId(Member member) {
    return MemberId.from(member.getId(), member.getNamespace());
  }

  @Override
  public <M> void broadcast(
      String subject,
      M message,
      Function<M, byte[]> encoder) {
    multicast(subject, message, encoder, membershipService.getMembers()
        .stream()
        .filter(node -> !Objects.equal(getMemberId(node), getMemberId(membershipService.getLocalMember())))
        .map(this::getMemberId)
        .collect(Collectors.toSet()));
  }

  @Override
  public <M> void broadcastIncludeSelf(
      String subject,
      M message,
      Function<M, byte[]> encoder) {
    multicast(subject, message, encoder, membershipService.getMembers()
        .stream()
        .map(this::getMemberId)
        .collect(Collectors.toSet()));
  }

  @Override
  public <M> CompletableFuture<Void> unicast(
      String subject,
      M message,
      Function<M, byte[]> encoder,
      MemberId toMemberId) {
    try {
      return doUnicast(subject, encoder.apply(message), toMemberId);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public <M> void multicast(
      String subject,
      M message,
      Function<M, byte[]> encoder,
      Set<MemberId> nodes) {
    byte[] payload = encoder.apply(message);
    nodes.forEach(memberId -> doUnicast(subject, payload, memberId));
  }

  @Override
  public <M, R> CompletableFuture<R> send(
      String subject,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      MemberId toMemberId,
      Duration timeout) {
    try {
      return sendAndReceive(subject, encoder.apply(message), toMemberId, timeout).thenApply(decoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  private CompletableFuture<Void> doUnicast(String subject, byte[] payload, MemberId toMemberId) {
    Member member = membershipService.getMember(toMemberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }
    return messagingService.sendAsync(Address.from(member.getHost(), member.getPort()), subject, payload);
  }

  private CompletableFuture<byte[]> sendAndReceive(String subject, byte[] payload, MemberId toMemberId, Duration timeout) {
    Member member = membershipService.getMember(toMemberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }
    return messagingService.sendAndReceive(Address.from(member.getHost(), member.getPort()), subject, payload, timeout);
  }

  @Override
  public void unsubscribe(String subject) {
    messagingService.unregisterHandler(subject);
  }

  @Override
  public <M, R> CompletableFuture<Void> subscribe(String subject,
      Function<byte[], M> decoder,
      Function<M, R> handler,
      Function<R, byte[]> encoder,
      Executor executor) {
    messagingService.registerHandler(subject,
        new InternalMessageResponder<M, R>(decoder, encoder, m -> {
          CompletableFuture<R> responseFuture = new CompletableFuture<>();
          executor.execute(() -> {
            try {
              responseFuture.complete(handler.apply(m));
            } catch (Exception e) {
              responseFuture.completeExceptionally(e);
            }
          });
          return responseFuture;
        }));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<Void> subscribe(String subject,
      Function<byte[], M> decoder,
      Function<M, CompletableFuture<R>> handler,
      Function<R, byte[]> encoder) {
    messagingService.registerHandler(subject, new InternalMessageResponder<>(decoder, encoder, handler));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M> CompletableFuture<Void> subscribe(String subject,
      Function<byte[], M> decoder,
      Consumer<M> handler,
      Executor executor) {
    messagingService.registerHandler(subject, new InternalMessageConsumer<>(decoder, handler), executor);
    return CompletableFuture.completedFuture(null);
  }

  private static class InternalMessageResponder<M, R> implements Function<byte[], CompletableFuture<byte[]>> {
    private final Function<byte[], M> decoder;
    private final Function<R, byte[]> encoder;
    private final Function<M, CompletableFuture<R>> handler;

    InternalMessageResponder(Function<byte[], M> decoder,
        Function<R, byte[]> encoder,
        Function<M, CompletableFuture<R>> handler) {
      this.decoder = decoder;
      this.encoder = encoder;
      this.handler = handler;
    }

    @Override
    public CompletableFuture<byte[]> apply(byte[] bytes) {
      return handler.apply(decoder.apply(bytes)).thenApply(encoder);
    }
  }

  private static class InternalMessageConsumer<M> implements Consumer<byte[]> {
    private final Function<byte[], M> decoder;
    private final Consumer<M> consumer;

    InternalMessageConsumer(Function<byte[], M> decoder, Consumer<M> consumer) {
      this.decoder = decoder;
      this.consumer = consumer;
    }

    @Override
    public void accept(byte[] bytes) {
      consumer.accept(decoder.apply(bytes));
    }
  }
}
