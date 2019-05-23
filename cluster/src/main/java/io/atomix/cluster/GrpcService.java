package io.atomix.cluster;

import io.grpc.BindableService;
import io.grpc.Channel;

/**
 * Service registry.
 */
public interface GrpcService {

  /**
   * Registers the given service.
   *
   * @param service the service to register
   */
  void register(BindableService service);

  /**
   * Gets a channel for the given node.
   *
   * @param host the node host
   * @param port the node port
   * @return the channel
   */
  Channel getChannel(String host, int port);

}
