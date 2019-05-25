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
package io.atomix.protocols.log;

import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocolBuilder;

/**
 * Log protocol builder.
 */
public class DistributedLogProtocolBuilder extends PrimitiveProtocolBuilder<DistributedLogProtocolBuilder, DistributedLogProtocolConfig, DistributedLogProtocol> {
  protected DistributedLogProtocolBuilder(DistributedLogProtocolConfig config) {
    super(config);
  }

  /**
   * Sets the number of partitions.
   *
   * @param numPartitions the number of partitions
   * @return the log protocol builder
   */
  public DistributedLogProtocolBuilder withNumPartitions(int numPartitions) {
    config.setPartitions(numPartitions);
    return this;
  }

  /**
   * Sets the replication factor.
   *
   * @param replicationFactor the replication factor
   * @return the log protocol builder
   */
  public DistributedLogProtocolBuilder withReplicationFactor(int replicationFactor) {
    config.setReplicationFactor(replicationFactor);
    return this;
  }

  /**
   * Sets the replication strategy.
   *
   * @param replicationStrategy the replication strategy
   * @return the log protocol builder
   */
  public DistributedLogProtocolBuilder withReplicationStrategy(ReplicationStrategy replicationStrategy) {
    config.setReplicationStrategy(replicationStrategy);
    return this;
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol builder
   */
  public DistributedLogProtocolBuilder withPartitioner(Partitioner<String> partitioner) {
    config.setPartitioner(partitioner);
    return this;
  }

  @Override
  public DistributedLogProtocol build() {
    return new DistributedLogProtocol(config);
  }
}
