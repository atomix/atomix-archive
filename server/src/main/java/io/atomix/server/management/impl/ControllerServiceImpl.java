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
