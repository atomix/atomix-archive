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
package io.atomix.node.management.impl;

import java.util.Collection;

import io.atomix.api.controller.NodeConfig;
import io.atomix.api.controller.PartitionConfig;
import io.atomix.api.controller.PartitionId;
import io.atomix.node.management.ConfigService;

/**
 * Configuration service.
 */
public class ConfigServiceImpl implements ConfigService {
  private final NodeConfig node;
  private final PartitionConfig config;

  public ConfigServiceImpl(String nodeId, PartitionConfig partitionConfig) {
    this.config = partitionConfig;
    this.node = partitionConfig.getMembersList().stream()
        .filter(member -> member.getId().equals(nodeId))
        .findFirst()
        .orElse(null);
  }

  @Override
  public PartitionId getPartition() {
    return config.getPartition();
  }

  @Override
  public NodeConfig getController() {
    return config.getController();
  }

  @Override
  public NodeConfig getNode() {
    return node;
  }

  @Override
  public Collection<NodeConfig> getCluster() {
    return config.getMembersList();
  }
}
