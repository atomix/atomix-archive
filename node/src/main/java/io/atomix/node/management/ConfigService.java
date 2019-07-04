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
package io.atomix.node.management;

import java.util.Collection;

import io.atomix.api.controller.NodeConfig;
import io.atomix.api.controller.PartitionId;

/**
 * Server configuration service.
 */
public interface ConfigService {

    /**
     * Returns the partition ID.
     *
     * @return the partition ID
     */
    PartitionId getPartition();

    /**
     * Returns the controller configuration.
     *
     * @return the controller configuration
     */
    NodeConfig getController();

    /**
     * Returns the node configuration.
     *
     * @return the node configuration
     */
    NodeConfig getNode();

    /**
     * Returns the cluster configuration.
     *
     * @return the cluster configuration
     */
    Collection<NodeConfig> getCluster();

}
