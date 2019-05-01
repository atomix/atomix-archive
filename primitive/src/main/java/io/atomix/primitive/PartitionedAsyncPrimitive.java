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
package io.atomix.primitive;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Partitioned primitive proxy.
 */
public abstract class PartitionedAsyncPrimitive<P extends AsyncPrimitive> implements AsyncPrimitive, ManagedAsyncPrimitive<P> {
  private final String name;
  private final PrimitiveType type;
  private final List<PartitionId> partitionIds = new CopyOnWriteArrayList<>();
  private final Map<PartitionId, P> partitions = new ConcurrentHashMap<>();
  private final Partitioner<String> partitioner;
  private final Set<Consumer<PrimitiveState>> stateChangeListeners = Sets.newCopyOnWriteArraySet();
  private final Map<PartitionId, PrimitiveState> states = Maps.newHashMap();
  private volatile PrimitiveState state = PrimitiveState.CLOSED;

  protected PartitionedAsyncPrimitive(
      String name, PrimitiveType type, Map<PartitionId, P> partitions, Partitioner<String> partitioner) {
    this.name = checkNotNull(name, "name cannot be null");
    this.type = checkNotNull(type, "type cannot be null");
    partitions.forEach((partitionId, partition) -> {
      partitionIds.add(partitionId);
      this.partitions.put(partitionId, partition);
    });
    Collections.sort(partitionIds, Comparator.comparingInt(PartitionId::getPartition));
    this.partitioner = checkNotNull(partitioner, "partitioner cannot be null");
    partitions.forEach((partitionId, partition) -> {
      partition.addStateChangeListener(state -> onStateChange(partitionId, state));
    });
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return type;
  }

  /**
   * Returns the set of partitions.
   *
   * @return the set of all partitions
   */
  protected Collection<P> getPartitions() {
    return partitions.values();
  }

  /**
   * Returns the sorted list of partition IDs.
   *
   * @return the sorted list of partition IDs
   */
  protected List<PartitionId> getPartitionIds() {
    return partitionIds;
  }

  /**
   * Returns the partition for the given key.
   *
   * @param key the key for which to return the partition
   * @return the partition ID for the given key
   */
  protected PartitionId getPartitionId(String key) {
    return partitioner.partition(key, getPartitionIds());
  }

  /**
   * Returns a partition by ID.
   *
   * @param partitionId the partition ID
   * @return the partition with the given ID
   */
  protected P getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  /**
   * Returns the partition proxy for the given key.
   *
   * @param key the key for which to return the partition proxy
   * @return the partition proxy for the given key
   */
  protected P getPartition(String key) {
    return getPartition(getPartitionId(key));
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.remove(listener);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<P> connect() {
    return Futures.allOf(partitions.values()
        .stream()
        .map(partition -> ((ManagedAsyncPrimitive<P>) partition).connect())
        .collect(Collectors.toList()))
        .thenApply(v -> (P) this);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.allOf(partitions.values()
        .stream()
        .map(AsyncPrimitive::delete)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return Futures.allOf(partitions.values()
        .stream()
        .map(AsyncPrimitive::close)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  /**
   * Handles a partition proxy state change.
   */
  private synchronized void onStateChange(PartitionId partitionId, PrimitiveState state) {
    states.put(partitionId, state);
    switch (state) {
      case CONNECTED:
        if (this.state != PrimitiveState.CONNECTED && !states.containsValue(PrimitiveState.SUSPENDED) && !states.containsValue(PrimitiveState.CLOSED)) {
          this.state = PrimitiveState.CONNECTED;
          stateChangeListeners.forEach(l -> l.accept(PrimitiveState.CONNECTED));
        }
        break;
      case SUSPENDED:
        if (this.state == PrimitiveState.CONNECTED) {
          this.state = PrimitiveState.SUSPENDED;
          stateChangeListeners.forEach(l -> l.accept(PrimitiveState.SUSPENDED));
        }
        break;
      case CLOSED:
        if (this.state != PrimitiveState.CLOSED) {
          this.state = PrimitiveState.CLOSED;
          stateChangeListeners.forEach(l -> l.accept(PrimitiveState.CLOSED));
        }
        break;
    }
  }
}