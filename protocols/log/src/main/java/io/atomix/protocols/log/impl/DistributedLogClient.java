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
package io.atomix.protocols.log.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.Maps;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Partitioned log client.
 */
public class DistributedLogClient implements LogClient {
  private final List<PartitionId> partitionIds = new CopyOnWriteArrayList<>();
  private final Map<Integer, LogSession> intPartitions = Maps.newConcurrentMap();
  private final Map<PartitionId, LogSession> partitions = Maps.newConcurrentMap();
  private final List<LogSession> sortedPartitions = new CopyOnWriteArrayList<>();
  private final Partitioner<String> partitioner;

  public DistributedLogClient(
      Map<PartitionId, LogSession> partitions,
      Partitioner<String> partitioner) {
    this.partitioner = checkNotNull(partitioner, "partitioner cannot be null");
    partitions.forEach((partitionId, partition) -> {
      this.partitionIds.add(partitionId);
      this.partitions.put(partitionId, partition);
      this.intPartitions.put(partitionId.getPartition(), partition);
      this.sortedPartitions.add(partition);
    });
  }

  @Override
  public Collection<LogSession> getPartitions() {
    return sortedPartitions;
  }

  @Override
  public Collection<PartitionId> getPartitionIds() {
    return partitions.keySet();
  }

  @Override
  public LogSession getPartition(int partitionId) {
    return intPartitions.get(partitionId);
  }

  @Override
  public LogSession getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public PartitionId getPartitionId(String key) {
    return partitioner.partition(key, partitionIds);
  }
}
