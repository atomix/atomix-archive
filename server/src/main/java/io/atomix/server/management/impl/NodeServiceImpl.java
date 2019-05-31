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
