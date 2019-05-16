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
package io.atomix.primitive;

import java.util.Collection;

import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;

/**
 * Primitive client.
 */
public interface PrimitiveClient {

  /**
   * Returns the partitioner for the client.
   *
   * @return the partitioner
   */
  Partitioner<String> getPartitioner();

  /**
   * Returns the collection of all partitions.
   *
   * @return the collection of all partitions
   */
  Collection<PartitionClient> getPartitions();

  /**
   * Returns the collection of all partition IDs.
   *
   * @return the collection of all partition IDs
   */
  Collection<PartitionId> getPartitionIds();

  /**
   * Returns the partition with the given identifier.
   *
   * @param partitionId the partition with the given identifier
   * @return the partition with the given identifier
   */
  PartitionClient getPartition(int partitionId);

  /**
   * Returns the partition with the given identifier.
   *
   * @param partitionId the partition with the given identifier
   * @return the partition with the given identifier
   */
  PartitionClient getPartition(PartitionId partitionId);

  /**
   * Returns the partition ID for the given key.
   *
   * @param key the key for which to return the partition ID
   * @return the partition ID for the given key
   */
  PartitionId getPartitionId(String key);

  /**
   * Returns the partition for the given key.
   *
   * @param key the key for which to return the partition
   * @return the partition for the given key
   */
  default PartitionClient getPartition(String key) {
    return getPartition(getPartitionId(key));
  }

}
