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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Strings;
import io.atomix.server.ServerConfig;
import io.atomix.server.management.Node;
import io.atomix.server.management.NodeService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;

/**
 * Node service implementation.
 */
@Component(ServerConfig.class)
public class NodeServiceImpl implements NodeService, Managed<ServerConfig> {
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final int DEFAULT_PORT = 5678;
  private Node node;

  @Override
  public Node getNode() {
    return node;
  }

  @Override
  public CompletableFuture<Void> start(ServerConfig config) {
    String id = !Strings.isNullOrEmpty(config.getNode().getId())
        ? config.getNode().getId()
        : UUID.randomUUID().toString();
    String host = !Strings.isNullOrEmpty(config.getNode().getHost())
        ? config.getNode().getHost()
        : DEFAULT_HOST;
    int port = config.getNode().getPort() != 0
        ? config.getNode().getPort()
        : DEFAULT_PORT;
    node = new Node(id, host, port);
    return CompletableFuture.completedFuture(null);
  }
}
