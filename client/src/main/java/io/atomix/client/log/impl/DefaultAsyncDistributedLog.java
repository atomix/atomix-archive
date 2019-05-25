/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.client.log.impl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import io.atomix.api.log.CreateTopicRequest;
import io.atomix.api.log.CreateTopicResponse;
import io.atomix.api.log.LogServiceGrpc;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.PrimitiveType;
import io.atomix.client.log.AsyncDistributedLog;
import io.atomix.client.log.AsyncDistributedLogPartition;
import io.atomix.client.log.DistributedLog;
import io.atomix.client.log.DistributedLogType;
import io.atomix.client.log.Record;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;
import io.grpc.stub.StreamObserver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default distributed log.
 */
public class DefaultAsyncDistributedLog<E> implements AsyncDistributedLog<E> {
  private static final BiFunction<String, List<Integer>, Integer> DEFAULT_PARTITIONER = (key, partitions) -> {
    int hash = Math.abs(Hashing.murmur3_32().hashUnencodedChars(key).asInt());
    return partitions.get(Hashing.consistentHash(hash, partitions.size()));
  };

  private final PrimitiveId id;
  private final LogServiceGrpc.LogServiceStub log;
  private final BiFunction<String, List<Integer>, Integer> partitioner;
  private final Map<Integer, DefaultAsyncDistributedLogPartition<E>> partitions = new ConcurrentHashMap<>();
  private final List<AsyncDistributedLogPartition<E>> sortedPartitions = new CopyOnWriteArrayList<>();
  private final List<Integer> partitionIds = new CopyOnWriteArrayList<>();
  private final Serializer serializer;

  public DefaultAsyncDistributedLog(PrimitiveId id, PrimitiveManagementService managementService, Serializer serializer) {
    this(id, managementService, DEFAULT_PARTITIONER, serializer);
  }

  public DefaultAsyncDistributedLog(PrimitiveId id, PrimitiveManagementService managementService, BiFunction<String, List<Integer>, Integer> partitioner, Serializer serializer) {
    this.id = checkNotNull(id);
    this.log = LogServiceGrpc.newStub(managementService.getChannelFactory().getChannel());
    this.partitioner = checkNotNull(partitioner);
    this.serializer = checkNotNull(serializer);
  }

  @Override
  public String name() {
    return id.getName();
  }

  @Override
  public PrimitiveType type() {
    return DistributedLogType.instance();
  }

  /**
   * Encodes the given object using the configured {@link #serializer}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  private <T> byte[] encode(T object) {
    return object != null ? serializer.encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #serializer}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  private <T> T decode(byte[] bytes) {
    return bytes != null ? serializer.decode(bytes) : null;
  }

  @Override
  public List<AsyncDistributedLogPartition<E>> getPartitions() {
    return sortedPartitions;
  }

  @Override
  public AsyncDistributedLogPartition<E> getPartition(int partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public AsyncDistributedLogPartition<E> getPartition(E entry) {
    return partitions.get(partitioner.apply(BaseEncoding.base16().encode(encode(entry)), partitionIds));
  }

  @Override
  public CompletableFuture<Void> produce(E entry) {
    byte[] bytes = encode(entry);
    return partitions.get(partitioner.apply(BaseEncoding.base16().encode(encode(entry)), partitionIds)).produce(bytes);
  }

  @Override
  public CompletableFuture<Void> consume(Consumer<Record<E>> consumer) {
    return Futures.allOf(getPartitions().stream()
        .map(partition -> partition.consume(consumer)))
        .thenApply(v -> null);
  }

  @Override
  public DistributedLog<E> sync(Duration operationTimeout) {
    return new BlockingDistributedLog<>(this, operationTimeout.toMillis());
  }

  /**
   * Connects the distributed log.
   *
   * @return a future to be completed once the log has connected
   */
  public CompletableFuture<AsyncDistributedLog<E>> connect() {
    return this.<CreateTopicResponse>execute(observer -> log.createTopic(CreateTopicRequest.newBuilder()
        .setId(id)
        .build(), observer))
        .thenApply(response -> {
          for (int i = 1; i <= response.getPartitions(); i++) {
            DefaultAsyncDistributedLogPartition<E> partition = new DefaultAsyncDistributedLogPartition<>(id, i, log, serializer);
            partitions.put(i, partition);
            sortedPartitions.add(partition);
            partitionIds.add(i);
          }
          return this;
        });
  }

  private <T> CompletableFuture<T> execute(Consumer<StreamObserver<T>> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    callback.accept(new StreamObserver<T>() {
      @Override
      public void onNext(T value) {
        future.complete(value);
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
        if (!future.isDone()) {
          future.complete(null);
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }
}
