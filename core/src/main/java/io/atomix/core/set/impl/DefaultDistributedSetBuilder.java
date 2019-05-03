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
package io.atomix.core.set.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.primitive.ManagedAsyncPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.serializer.Serializer;

/**
 * Default distributed set builder.
 *
 * @param <E> type for set elements
 */
public class DefaultDistributedSetBuilder<E> extends DistributedSetBuilder<E> {
  public DefaultDistributedSetBuilder(String name, DistributedSetConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedSet<E>> buildAsync() {
    return managementService.getPrimitiveRegistry().createPrimitive(name, type)
        .thenApply(v -> newMultitonProxies(SetService.TYPE, SetProxy::new))
        .thenApply(proxies -> proxies.entrySet().stream()
            .map(entry -> Maps.<PartitionId, AsyncDistributedSet<String>>immutableEntry(
                entry.getKey(),
                new RawAsyncDistributedSet(entry.getValue(), config.getSessionTimeout(), managementService)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        .thenApply(partitions -> new PartitionedAsyncDistributedSet(name, type, partitions, Partitioner.MURMUR3))
        .thenCompose(ManagedAsyncPrimitive::connect)
        .thenApply(rawSet -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncDistributedSet<E, String>(
              rawSet,
              element -> BaseEncoding.base16().encode(serializer.encode(element)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));
        })
        .<AsyncDistributedSet<E>>thenApply(set -> {
          if (config.getCacheConfig().isEnabled()) {
            return new CachingAsyncDistributedSet<>(set, config.getCacheConfig());
          }
          return set;
        })
        .thenApply(set -> {
          if (config.isReadOnly()) {
            return new UnmodifiableAsyncDistributedSet<>(set);
          }
          return set;
        })
        .thenApply(AsyncDistributedSet::sync);
  }
}
