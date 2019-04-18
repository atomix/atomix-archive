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
package io.atomix.core.log.impl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.google.common.io.BaseEncoding;
import io.atomix.core.log.AsyncDistributedLog;
import io.atomix.core.log.AsyncDistributedLogPartition;
import io.atomix.core.log.DistributedLog;
import io.atomix.core.log.DistributedLogType;
import io.atomix.core.log.Record;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.log.LogSession;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default distributed log.
 */
public class DefaultAsyncDistributedLog<E> implements AsyncDistributedLog<E> {
  private final String name;
  private final LogClient client;
  private final Map<Integer, DefaultAsyncDistributedLogPartition<E>> partitions = new ConcurrentHashMap<>();
  private final List<AsyncDistributedLogPartition<E>> sortedPartitions = new CopyOnWriteArrayList<>();
  private final Serializer serializer;

  public DefaultAsyncDistributedLog(String name, LogClient client, Serializer serializer) {
    this.name = checkNotNull(name);
    this.client = checkNotNull(client);
    this.serializer = checkNotNull(serializer);
    client.getPartitionIds().forEach(partitionId -> {
      LogSession partition = client.getPartition(partitionId);
      DefaultAsyncDistributedLogPartition<E> logPartition = new DefaultAsyncDistributedLogPartition<>(this, partitionId, partition, serializer);
      partitions.put(partitionId.getPartition(), logPartition);
      sortedPartitions.add(logPartition);
    });
  }

  @Override
  public String name() {
    return name;
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
    return partitions.get(client.getPartitionId(BaseEncoding.base16().encode(encode(entry))).getPartition());
  }

  @Override
  public CompletableFuture<Void> produce(E entry) {
    byte[] bytes = encode(entry);
    return partitions.get(client.getPartitionId(BaseEncoding.base16().encode(bytes)).getPartition()).produce(bytes);
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

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return CompletableFuture.completedFuture(null);
  }
}
