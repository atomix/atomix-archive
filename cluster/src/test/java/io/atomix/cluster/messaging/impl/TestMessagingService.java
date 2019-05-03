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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.Sets;
import io.atomix.cluster.messaging.MessagingException.NoRemoteHandler;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.utils.TriConsumer;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;
import io.atomix.utils.stream.StreamFunction;
import io.atomix.utils.stream.StreamHandler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Test messaging service.
 */
public class TestMessagingService implements MessagingService {
  private final Address address;
  private final Map<Address, TestMessagingService> services;
  private final Map<String, BiFunction<Address, ?, ?>> handlers = new ConcurrentHashMap<>();
  private final Set<Address> partitions = Sets.newConcurrentHashSet();

  public TestMessagingService(Address address, Map<Address, TestMessagingService> services) {
    this.address = address;
    this.services = services;
    services.put(address, this);
  }

  /**
   * Returns the test service for the given address or {@code null} if none has been created.
   */
  private TestMessagingService getService(Address address) {
    checkNotNull(address);
    return services.get(address);
  }

  /**
   * Returns the given handler for the given address.
   */
  private <T, U> BiFunction<Address, T, CompletableFuture<U>> getHandler(Address address, String type) {
    TestMessagingService service = getService(address);
    if (service == null) {
      return (e, p) -> Futures.exceptionalFuture(new NoRemoteHandler());
    }
    BiFunction handler = service.handlers.get(checkNotNull(type));
    if (handler == null) {
      return (e, p) -> Futures.exceptionalFuture(new NoRemoteHandler());
    }
    return handler;
  }

  /**
   * Partitions the node from the given address.
   */
  void partition(Address address) {
    partitions.add(address);
  }

  /**
   * Heals the partition from the given address.
   */
  void heal(Address address) {
    partitions.remove(address);
  }

  /**
   * Returns a boolean indicating whether this node is partitioned from the given address.
   *
   * @param address the address to check
   * @return whether this node is partitioned from the given address
   */
  boolean isPartitioned(Address address) {
    return partitions.contains(address);
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public CompletableFuture<Void> sendAsync(Address address, String type, byte[] payload, boolean keepAlive) {
    if (isPartitioned(address)) {
      return Futures.exceptionalFuture(new ConnectException());
    }
    return getHandler(address, type).apply(this.address, payload).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Address address, String type, byte[] payload, boolean keepAlive) {
    if (isPartitioned(address)) {
      return Futures.exceptionalFuture(new ConnectException());
    }
    return this.<byte[], byte[]>getHandler(address, type).apply(this.address, payload);
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Address address, String type, byte[] payload, boolean keepAlive, Executor executor) {
    if (isPartitioned(address)) {
      return Futures.exceptionalFuture(new ConnectException());
    }
    ComposableFuture<byte[]> future = new ComposableFuture<>();
    sendAndReceive(address, type, payload).whenCompleteAsync(future, executor);
    return future;
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Address address, String type, byte[] payload, boolean keepAlive, Duration timeout) {
    if (isPartitioned(address)) {
      return Futures.exceptionalFuture(new ConnectException());
    }
    return this.<byte[], byte[]>getHandler(address, type).apply(this.address, payload);
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Address address, String type, byte[] payload, boolean keepAlive, Duration timeout, Executor executor) {
    if (isPartitioned(address)) {
      return Futures.exceptionalFuture(new ConnectException());
    }
    ComposableFuture<byte[]> future = new ComposableFuture<>();
    sendAndReceive(address, type, payload).whenCompleteAsync(future, executor);
    return future;
  }

  @Override
  public CompletableFuture<StreamHandler<byte[]>> sendStreamAsync(Address address, String type) {
    return null;
  }

  @Override
  public CompletableFuture<StreamFunction<byte[], CompletableFuture<byte[]>>> sendStreamAndReceive(Address address, String type, Duration timeout, Executor executor) {
    return null;
  }

  @Override
  public CompletableFuture<Void> sendAndReceiveStream(Address address, String type, byte[] payload, StreamHandler<byte[]> handler, Duration timeout, Executor executor) {
    return null;
  }

  @Override
  public CompletableFuture<StreamHandler<byte[]>> sendStreamAndReceiveStream(Address address, String type, StreamHandler<byte[]> handler, Duration timeout, Executor executor) {
    return null;
  }

  @Override
  public void registerHandler(String type, BiConsumer<Address, byte[]> handler, Executor executor) {
    checkNotNull(type);
    checkNotNull(handler);
    handlers.put(type, (e, p) -> {
      try {
        executor.execute(() -> handler.accept(e, (byte[]) p));
        return CompletableFuture.completedFuture(new byte[0]);
      } catch (RejectedExecutionException e2) {
        return Futures.exceptionalFuture(e2);
      }
    });
  }

  @Override
  public void registerHandler(String type, BiFunction<Address, byte[], byte[]> handler, Executor executor) {
    checkNotNull(type);
    checkNotNull(handler);
    handlers.put(type, (e, p) -> {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      try {
        executor.execute(() -> future.complete(handler.apply(e, (byte[]) p)));
      } catch (RejectedExecutionException e2) {
        future.completeExceptionally(e2);
      }
      return future;
    });
  }

  @Override
  public void registerHandler(String type, BiFunction<Address, byte[], CompletableFuture<byte[]>> handler) {
    checkNotNull(type);
    checkNotNull(handler);
    handlers.put(type, handler);
  }

  @Override
  public void registerStreamHandler(String type, Function<Address, StreamFunction<byte[], CompletableFuture<byte[]>>> handler) {

  }

  @Override
  public void registerStreamingHandler(String type, TriConsumer<Address, byte[], StreamHandler<byte[]>> handler) {

  }

  @Override
  public void registerStreamingStreamHandler(String type, BiFunction<Address, StreamHandler<byte[]>, StreamHandler<byte[]>> handler) {

  }

  @Override
  public void unregisterHandler(String type) {
    handlers.remove(checkNotNull(type));
  }
}
