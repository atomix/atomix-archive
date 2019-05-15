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

import java.time.Duration;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.Recovery;
import io.atomix.log.protocol.Replication;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocolConfig;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log protocol configuration.
 */
public class DistributedLogProtocolConfig extends PrimitiveProtocolConfig<DistributedLogProtocolConfig> {
  private static final int DEFAULT_PARTITIONS = 1;
  private static final int DEFAULT_REPLICATION_FACTOR = 1;
  private static final Replication DEFAULT_REPLICATION_STRATEGY = Replication.ASYNCHRONOUS;

  private String group;
  private int partitions = DEFAULT_PARTITIONS;
  private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
  private Replication replicationStrategy = DEFAULT_REPLICATION_STRATEGY;
  private Partitioner<String> partitioner = Partitioner.MURMUR3;

  @Override
  public PrimitiveProtocol.Type getType() {
    return DistributedLogProtocol.TYPE;
  }

  /**
   * Returns the partition group.
   *
   * @return the partition group
   */
  public String getGroup() {
    return group;
  }

  /**
   * Sets the partition group.
   *
   * @param group the partition group
   * @return the protocol configuration
   */
  public DistributedLogProtocolConfig setGroup(String group) {
    this.group = group;
    return this;
  }

  /**
   * Returns the protocol partitioner.
   *
   * @return the protocol partitioner
   */
  public Partitioner<String> getPartitioner() {
    return partitioner;
  }

  /**
   * Sets the protocol partitioner.
   *
   * @param partitioner the protocol partitioner
   * @return the protocol configuration
   */
  public DistributedLogProtocolConfig setPartitioner(Partitioner<String> partitioner) {
    this.partitioner = partitioner;
    return this;
  }

  /**
   * Returns the number of partitions for the topic.
   *
   * @return the number of partitions for the topic
   */
  public int getPartitions() {
    return partitions;
  }

  /**
   * Sets the number of partitions for the topic.
   *
   * @param partitions the number of partitions for the topic
   * @return the log protocol configuration
   */
  public DistributedLogProtocolConfig setPartitions(int partitions) {
    checkArgument(partitions > 0, "partitions must be positive");
    this.partitions = partitions;
    return this;
  }

  /**
   * Returns the replication factor.
   *
   * @return the replication factor
   */
  public int getReplicationFactor() {
    return replicationFactor;
  }

  /**
   * Sets the replication factor.
   *
   * @param replicationFactor the replication factor
   * @return the log protocol configuration
   * @throws IllegalArgumentException if the replication factor is negative
   */
  public DistributedLogProtocolConfig setReplicationFactor(int replicationFactor) {
    checkArgument(replicationFactor >= 0);
    this.replicationFactor = replicationFactor;
    return this;
  }

  /**
   * Returns the replication strategy.
   *
   * @return the replication strategy
   */
  public Replication getReplicationStrategy() {
    return replicationStrategy;
  }

  /**
   * Sets the replication strategy.
   *
   * @param replicationStrategy the replication strategy
   * @return the log protocol configuration
   */
  public DistributedLogProtocolConfig setReplicationStrategy(Replication replicationStrategy) {
    checkNotNull(replicationStrategy);
    this.replicationStrategy = replicationStrategy;
    return this;
  }
}
