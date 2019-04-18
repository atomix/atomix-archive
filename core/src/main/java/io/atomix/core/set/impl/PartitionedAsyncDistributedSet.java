/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.core.set.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.PartitionedAsyncIterator;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PartitionedAsyncPrimitive;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;

/**
 * Partitioned asynchronous distributed set.
 */
public class PartitionedAsyncDistributedSet extends PartitionedAsyncPrimitive<AsyncDistributedSet<String>> implements AsyncDistributedSet<String> {
  public PartitionedAsyncDistributedSet(
      String name,
      PrimitiveType type,
      Map<PartitionId, AsyncDistributedSet<String>> partitions,
      Partitioner<String> partitioner) {
    super(name, type, partitions, partitioner);
  }

  @Override
  public CompletableFuture<Boolean> add(String element) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> remove(String element) {
    return null;
  }

  @Override
  public CompletableFuture<Integer> size() {
    return Futures.allOf(getPartitions().stream()
        .map(AsyncDistributedSet::size))
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return Futures.allOf(getPartitions().stream()
        .map(AsyncDistributedSet::isEmpty))
        .thenApply(results -> results.reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return Futures.allOf(getPartitions().stream().map(set -> set.clear())).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Boolean> contains(String element) {
    return getPartition(element).contains(element);
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
    Map<PartitionId, Collection<String>> partitions = new HashMap<>();
    c.forEach(key -> partitions.computeIfAbsent(getPartitionId(key), id -> new ArrayList<>()).add(key));
    return Futures.allOf(partitions.entrySet().stream()
        .map(entry -> getPartition(entry.getKey()).addAll(entry.getValue())))
        .thenApply(results -> results.reduce(Boolean::logicalOr).orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
    Map<PartitionId, Collection<String>> partitions = new HashMap<>();
    c.forEach(key -> partitions.computeIfAbsent(getPartitionId(key), id -> new ArrayList<>()).add(key));
    return Futures.allOf(partitions.entrySet().stream()
        .map(entry -> getPartition(entry.getKey()).containsAll(entry.getValue())))
        .thenApply(results -> results.reduce(Boolean::logicalOr).orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
    Map<PartitionId, Collection<String>> partitions = new HashMap<>();
    c.forEach(key -> partitions.computeIfAbsent(getPartitionId(key), id -> new ArrayList<>()).add(key));
    return Futures.allOf(partitions.entrySet().stream()
        .map(entry -> getPartition(entry.getKey()).removeAll(entry.getValue())))
        .thenApply(results -> results.reduce(Boolean::logicalOr).orElse(false));
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.addListener(listener, executor)))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.removeListener(listener)))
        .thenApply(v -> null);
  }

  @Override
  public AsyncIterator<String> iterator() {
    return new PartitionedAsyncIterator<>(getPartitions().stream()
        .map(partition -> partition.iterator())
        .iterator());
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<String>> transactionLog) {
    Map<PartitionId, List<SetUpdate<String>>> updatesGroupedBySet = Maps.newIdentityHashMap();
    transactionLog.records().forEach(update -> {
      updatesGroupedBySet.computeIfAbsent(getPartitionId(update.element()), k -> Lists.newLinkedList()).add(update);
    });
    Map<PartitionId, TransactionLog<SetUpdate<String>>> transactionsBySet =
        Maps.transformValues(updatesGroupedBySet, list -> new TransactionLog<>(transactionLog.transactionId(), transactionLog.version(), list));

    return Futures.allOf(transactionsBySet.entrySet()
        .stream()
        .map(e -> getPartition(e.getKey()).prepare(e.getValue()))
        .collect(Collectors.toList()))
        .thenApply(list -> list.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.commit(transactionId))).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.rollback(transactionId))).thenApply(v -> null);
  }

  @Override
  public DistributedSet<String> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}