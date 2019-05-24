package io.atomix.cluster.grpc;

import io.grpc.Channel;

/**
 * Service registry.
 */
public interface ChannelService {

  /**
   * Gets a channel for the given target.
   *
   * @param target the target
   * @return the channel
   */
  Channel getChannel(String target);

  /**
   * Gets a channel for the given node.
   *
   * @param host the node host
   * @param port the node port
   * @return the channel
   */
  Channel getChannel(String host, int port);

}
