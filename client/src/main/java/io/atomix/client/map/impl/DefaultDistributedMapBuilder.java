/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.client.map.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import io.atomix.api.headers.Name;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.map.AsyncAtomicMap;
import io.atomix.client.map.AsyncDistributedMap;
import io.atomix.client.map.DistributedMap;
import io.atomix.client.map.DistributedMapBuilder;
import io.atomix.client.utils.serializer.Serializer;

/**
 * Default distributed map builder.
 */
public class DefaultDistributedMapBuilder<K, V> extends DistributedMapBuilder<K, V> {
  public DefaultDistributedMapBuilder(Name name, PrimitiveManagementService managementService) {
    super(name, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedMap<K, V>> buildAsync() {
    return managementService.getPartitionService().getPartitionGroup(group)
        .thenCompose(group -> {
          Map<Integer, AsyncAtomicMap<String, byte[]>> partitions = group.getPartitions().stream()
              .map(partition -> Maps.immutableEntry(partition.id(), new DefaultAsyncAtomicMap(getName(), partition, managementService.getThreadFactory().createContext(), sessionTimeout)))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          return new PartitionedAsyncAtomicMap(name, partitions, partitioner).connect();
        })
        .thenApply(rawMap -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicMap<K, V, String, byte[]>(
              rawMap,
              key -> BaseEncoding.base16().encode(serializer.encode(key)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)),
              value -> serializer.encode(value),
              bytes -> serializer.decode(bytes));
        })
        .thenApply(map -> {
          if (cacheEnabled) {
            return new CachingAsyncAtomicMap<>(map, cacheSize);
          }
          return map;
        })
        .<AsyncAtomicMap<K, V>>thenApply(map -> {
          if (readOnly) {
            return new UnmodifiableAsyncAtomicMap<>(map);
          }
          return map;
        })
        .thenApply(DelegatingAsyncDistributedMap::new)
        .thenApply(AsyncDistributedMap::sync);
  }
}
