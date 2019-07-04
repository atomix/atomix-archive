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

/**
 * Cluster service.
 */
public interface ClusterService {

    /**
     * Returns the local node.
     *
     * @return the local node
     */
    Node getLocalNode();

    /**
     * Returns the controller node.
     *
     * @return the controller node
     */
    Node getControllerNode();

    /**
     * Returns a collection of nodes in the cluster.
     *
     * @return a collection of nodes in the cluster
     */
    Collection<Node> getNodes();

    /**
     * Returns a node by ID.
     *
     * @param id the node ID
     * @return the node
     */
    Node getNode(String id);

}
