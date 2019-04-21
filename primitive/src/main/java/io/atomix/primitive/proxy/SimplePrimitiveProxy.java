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
package io.atomix.primitive.proxy;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.client.PrimitiveClient;
import io.atomix.primitive.client.impl.DefaultPrimitiveClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Base class for primitive proxies.
 */
public abstract class SimplePrimitiveProxy implements PrimitiveProxy {
  private final ServiceId serviceId;
  private final PrimitiveClient client;
  private final ThreadContext context;

  protected SimplePrimitiveProxy(
      ServiceId serviceId,
      PartitionId partitionId,
      PrimitiveManagementService managementService,
      ThreadContext context) {
    this.serviceId = serviceId;
    this.client = new DefaultPrimitiveClient(serviceId, managementService.getPartitionService()
        .getPartitionGroup(partitionId.getGroup())
        .getPartition(partitionId)
        .getClient(), context);
    this.context = context;
  }

  @Override
  public ThreadContext context() {
    return context;
  }

  /**
   * Returns the primitive client.
   *
   * @return the primitive client
   */
  protected PrimitiveClient getClient() {
    return client;
  }

  /**
   * Returns the service ID.
   *
   * @return the service ID
   */
  protected ServiceId getServiceId() {
    return serviceId;
  }
}