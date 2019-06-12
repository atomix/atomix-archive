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
package io.atomix.server.management.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.api.controller.NodeConfig;
import io.atomix.server.management.ClusterService;
import io.atomix.server.management.ConfigService;
import io.atomix.server.management.Node;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Cluster service implementation.
 */
@Component
public class ClusterServiceImpl implements ClusterService, Managed {
  @Dependency
  private ConfigService configService;

  private Node localNode;
  private Node controllerNode;
  private final Map<String, Node> nodes = new ConcurrentHashMap<>();

  @Override
  public Node getLocalNode() {
    return localNode;
  }

  @Override
  public Node getControllerNode() {
    return controllerNode;
  }

  @Override
  public Collection<Node> getNodes() {
    return nodes.values();
  }

  @Override
  public Node getNode(String id) {
    return nodes.get(id);
  }

  @Override
  public CompletableFuture<Void> start() {
    localNode = new Node(
        configService.getNode().getId(),
        configService.getNode().getHost(),
        configService.getNode().getPort());
    controllerNode = new Node(
        configService.getController().getId(),
        configService.getController().getHost(),
        configService.getController().getPort());
    for (NodeConfig node : configService.getCluster()) {
      nodes.put(node.getId(), new Node(
          node.getId(),
          node.getHost(),
          node.getPort()));
    }
    return CompletableFuture.completedFuture(null);
  }
}
