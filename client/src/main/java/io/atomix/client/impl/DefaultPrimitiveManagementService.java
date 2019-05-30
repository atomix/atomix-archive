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
package io.atomix.client.impl;

import io.atomix.client.PrimitiveCache;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.partition.PartitionService;
import io.atomix.client.utils.concurrent.ThreadContextFactory;

/**
 * Default primitive management service.
 */
public class DefaultPrimitiveManagementService implements PrimitiveManagementService {
  private final PartitionService partitionService;
  private final PrimitiveCache primitiveCache;
  private final ThreadContextFactory threadContextFactory;

  public DefaultPrimitiveManagementService(
      PartitionService partitionService,
      PrimitiveCache primitiveCache,
      ThreadContextFactory threadContextFactory) {
    this.partitionService = partitionService;
    this.primitiveCache = primitiveCache;
    this.threadContextFactory = threadContextFactory;
  }

  @Override
  public PartitionService getPartitionService() {
    return partitionService;
  }

  @Override
  public PrimitiveCache getPrimitiveCache() {
    return primitiveCache;
  }

  @Override
  public ThreadContextFactory getThreadFactory() {
    return threadContextFactory;
  }
}
