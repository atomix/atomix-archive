/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.client.log;

import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.Partitioner;
import io.atomix.client.PrimitiveBuilder;
import io.atomix.client.PrimitiveManagementService;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Builder for DistributedLog.
 */
public abstract class DistributedLogBuilder<E> extends PrimitiveBuilder<DistributedLogBuilder<E>, DistributedLog<E>> {
  protected Partitioner<String> partitioner = Partitioner.MURMUR3;

  public DistributedLogBuilder(PrimitiveId id, PrimitiveManagementService managementService) {
    super(id, managementService);
  }

  /**
   * Sets the log partitioner.
   *
   * @param partitioner the log partitioner
   * @return the log builder
   */
  public DistributedLogBuilder<E> withPartitioner(Partitioner<String> partitioner) {
    this.partitioner = checkNotNull(partitioner);
    return this;
  }
}
