/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.client.set.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import io.atomix.api.headers.Name;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.set.AsyncDistributedSet;
import io.atomix.client.set.DistributedSet;
import io.atomix.client.set.DistributedSetBuilder;
import io.atomix.client.utils.serializer.Serializer;

/**
 * Default distributed set builder.
 *
 * @param <E> type for set elements
 */
public class DefaultDistributedSetBuilder<E> extends DistributedSetBuilder<E> {
  public DefaultDistributedSetBuilder(Name name, PrimitiveManagementService managementService) {
    super(name, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedSet<E>> buildAsync() {
    return managementService.getPartitionService().getPartitionGroup(group)
        .thenCompose(group -> {
          Map<Integer, AsyncDistributedSet<String>> partitions = group.getPartitions().stream()
              .map(partition -> Maps.immutableEntry(partition.id(), new DefaultAsyncDistributedSet(
                  getName(), partition, managementService.getThreadFactory().createContext(), sessionTimeout)))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          return new PartitionedAsyncDistributedSet(name, partitions, partitioner).connect();
        })
        .thenApply(rawSet -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncDistributedSet<E, String>(
              rawSet,
              element -> BaseEncoding.base16().encode(serializer.encode(element)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));
        })
        .<AsyncDistributedSet<E>>thenApply(set -> {
          if (cacheEnabled) {
            return new CachingAsyncDistributedSet<>(set, cacheSize);
          }
          return set;
        })
        .thenApply(set -> {
          if (readOnly) {
            return new UnmodifiableAsyncDistributedSet<>(set);
          }
          return set;
        })
        .thenApply(AsyncDistributedSet::sync);
  }
}
