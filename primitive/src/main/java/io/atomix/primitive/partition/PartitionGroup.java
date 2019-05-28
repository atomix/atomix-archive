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
package io.atomix.primitive.partition;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import com.google.common.hash.Hashing;
import com.google.protobuf.Descriptors;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.utils.ConfiguredType;
import io.atomix.utils.config.Configured;

/**
 * Primitive partition group.
 */
public interface PartitionGroup extends Configured<PartitionGroupConfig> {

  /**
   * Partition group type.
   */
  interface Type extends ConfiguredType<PartitionGroupConfig> {

    /**
     * Returns the partition group configuration type.
     *
     * @return the partition group configuration type
     */
    Class<?> getConfigType();

    /**
     * Returns the partition group configuration descriptor.
     *
     * @return the partition group configuration descriptor
     */
    Descriptors.Descriptor getConfigDescriptor();

    /**
     * Returns the protocol type.
     *
     * @return the protocol type
     */
    Class<?> getProtocolType();

    /**
     * Returns the partition group's protocol descriptor.
     *
     * @return the partition group's protocol descriptor
     */
    Descriptors.Descriptor getProtocolDescriptor();

    /**
     * Creates a new partition group instance.
     *
     * @param config the partition group configuration
     * @return the partition group
     */
    ManagedPartitionGroup newPartitionGroup(PartitionGroupConfig config);
  }

  /**
   * Returns the partition group name.
   *
   * @return the partition group name
   */
  String name();

  /**
   * Returns the partition group type.
   *
   * @return the partition group type
   */
  Type type();

  /**
   * Returns the primitive protocol type supported by the partition group.
   *
   * @return the primitive protocol type supported by the partition group
   */
  PrimitiveProtocol.Type protocol();

  /**
   * Returns a new primitive protocol.
   *
   * @return a new primitive protocol
   */
  ServiceProtocol newProtocol();

  /**
   * Returns a partition by ID.
   *
   * @param partitionId the partition identifier
   * @return the partition or {@code null} if no partition with the given identifier exists
   * @throws NullPointerException if the partition identifier is {@code null}
   */
  Partition getPartition(PartitionId partitionId);

  /**
   * Returns the partition for the given key.
   *
   * @param key the key for which to return the partition
   * @return the partition for the given key
   */
  default Partition getPartition(String key) {
    int hashCode = Hashing.sha256().hashString(key, StandardCharsets.UTF_8).asInt();
    return getPartition(getPartitionIds().get(Math.abs(hashCode) % getPartitionIds().size()));
  }

  /**
   * Returns a collection of all partitions.
   *
   * @return a collection of all partitions
   */
  Collection<Partition> getPartitions();

  /**
   * Returns a sorted list of partition IDs.
   *
   * @return a sorted list of partition IDs
   */
  List<PartitionId> getPartitionIds();
}
