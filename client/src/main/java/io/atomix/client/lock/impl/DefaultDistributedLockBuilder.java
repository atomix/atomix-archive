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
package io.atomix.client.lock.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.lock.AsyncDistributedLock;
import io.atomix.client.lock.DistributedLock;
import io.atomix.client.lock.DistributedLockBuilder;

/**
 * Default distributed lock builder implementation.
 */
public class DefaultDistributedLockBuilder extends DistributedLockBuilder {
  public DefaultDistributedLockBuilder(PrimitiveId id, PrimitiveManagementService managementService) {
    super(id, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<DistributedLock> buildAsync() {
    return managementService.getPartitionService().getPartitionGroup(group)
        .thenCompose(group -> new DefaultAsyncAtomicLock(
            getPrimitiveId(),
            group.getPartition(partitioner.partition(getPrimitiveId().getName(), group.getPartitionIds())),
            managementService.getThreadFactory().createContext(),
            sessionTimeout)
            .connect()
            .thenApply(DelegatingAsyncDistributedLock::new)
            .thenApply(AsyncDistributedLock::sync));
  }
}
