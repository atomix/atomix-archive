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
package io.atomix.core.counter.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.core.counter.AsyncDistributedCounter;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.core.counter.DistributedCounterBuilder;
import io.atomix.core.counter.DistributedCounterConfig;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;

/**
 * Default distributed counter builder.
 */
public class DefaultDistributedCounterBuilder extends DistributedCounterBuilder {
  public DefaultDistributedCounterBuilder(String name, DistributedCounterConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  public CompletableFuture<DistributedCounter> buildAsync() {
    PrimitiveProtocol protocol = protocol();
    return managementService.getPrimitiveRegistry().createPrimitive(name, type)
        .thenCompose(v -> {
          Partition partition = managementService.getPartitionService()
              .getPartitionGroup((ProxyProtocol) protocol)
              .getPartition(name);
          return ((ProxyProtocol) protocol).newClient(name, type, partition, managementService).connect();
        })
        .thenApply(CounterProxy::new)
        .thenApply(DefaultAsyncAtomicCounter::new)
        .thenApply(DelegatingDistributedCounter::new)
        .thenApply(AsyncDistributedCounter::sync);
  }
}
