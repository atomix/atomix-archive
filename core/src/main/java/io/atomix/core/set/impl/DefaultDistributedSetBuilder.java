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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.io.BaseEncoding;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetBuilder;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.concurrent.Futures;
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
    PrimitiveProtocol protocol = protocol();
    return managementService.getPrimitiveRegistry().createPrimitive(name, type)
        .thenCompose(v -> {
          Map<PartitionId, AsyncDistributedSet<String>> partitions = new HashMap<>();
          return Futures.allOf(managementService.getPartitionService().getPartitionGroup((ProxyProtocol) protocol()).getPartitions().stream()
              .map(partition -> ((ProxyProtocol) protocol).newClient(name, type, partition, managementService).connect()
                  .thenApply(session -> new SetProxy(session))
                  .thenApply(proxy -> partitions.put(partition.id(), new RawAsyncDistributedSet(proxy)))))
              .thenApply(w -> partitions);
        })
        .thenApply(partitions -> new PartitionedAsyncDistributedSet(name, type, partitions, Partitioner.MURMUR3))
        .thenApply(rawSet -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncDistributedSet<E, String>(
              rawSet,
              element -> BaseEncoding.base16().encode(serializer.encode(element)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));
        })
        .thenApply(set -> {
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
