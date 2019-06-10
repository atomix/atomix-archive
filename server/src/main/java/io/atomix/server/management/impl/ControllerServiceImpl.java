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

import java.util.concurrent.CompletableFuture;

import io.atomix.server.NodeConfig;
import io.atomix.server.ServerConfig;
import io.atomix.server.management.ChannelService;
import io.atomix.server.management.ControllerService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.grpc.Channel;

/**
 * Controller service implementation.
 */
@Component(ServerConfig.class)
public class ControllerServiceImpl implements ControllerService, Managed<ServerConfig> {
  @Dependency
  private ChannelService channelService;
  private NodeConfig controllerNode;

  @Override
  public Channel getChannel() {
    return channelService.getChannel(controllerNode.getHost(), controllerNode.getPort());
  }

  @Override
  public CompletableFuture<Void> start(ServerConfig config) {
    this.controllerNode = config.getController();
    return CompletableFuture.completedFuture(null);
  }
}
