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
package io.atomix.primitive.partition;

import java.util.HashMap;
import java.util.Map;

import io.atomix.utils.config.Config;

/**
 * Partition groups configuration.
 */
public class PartitionGroupsConfig implements Config {
  private static final String SYSTEM_GROUP_NAME = "system";

  private PartitionGroupConfig systemGroup;
  private Map<String, PartitionGroupConfig<?>> partitionGroups = new HashMap<>();

  /**
   * Returns the system management partition group.
   *
   * @return the system management partition group
   */
  public PartitionGroupConfig<?> getSystemGroup() {
    return systemGroup;
  }

  /**
   * Sets the system management partition group.
   *
   * @param managementGroup the system management partition group
   * @return the Atomix configuration
   */
  public PartitionGroupsConfig setSystemGroup(PartitionGroupConfig<?> managementGroup) {
    managementGroup.setName(SYSTEM_GROUP_NAME);
    this.systemGroup = managementGroup;
    return this;
  }

  /**
   * Returns the partition group configurations.
   *
   * @return the partition group configurations
   */
  public Map<String, PartitionGroupConfig<?>> getPartitionGroups() {
    return partitionGroups;
  }

  /**
   * Sets the partition group configurations.
   *
   * @param partitionGroups the partition group configurations
   * @return the Atomix configuration
   */
  public PartitionGroupsConfig setPartitionGroups(Map<String, PartitionGroupConfig<?>> partitionGroups) {
    partitionGroups.forEach((name, group) -> group.setName(name));
    this.partitionGroups = partitionGroups;
    return this;
  }

  /**
   * Adds a partition group configuration.
   *
   * @param partitionGroup the partition group configuration to add
   * @return the Atomix configuration
   */
  public PartitionGroupsConfig addPartitionGroup(PartitionGroupConfig partitionGroup) {
    partitionGroups.put(partitionGroup.getName(), partitionGroup);
    return this;
  }
}
