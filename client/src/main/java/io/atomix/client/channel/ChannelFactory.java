package io.atomix.client.channel;

import io.grpc.Channel;

/**
 * Channel factory.
 */
@FunctionalInterface
public interface ChannelFactory {

  /**
   * Returns a new gRPC channel for the client.
   *
   * @return the gRPC channel
   */
  Channel getChannel();

}
