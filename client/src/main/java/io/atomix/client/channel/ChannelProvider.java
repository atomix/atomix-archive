package io.atomix.client.channel;

/**
 * Channel provider.
 */
public interface ChannelProvider {

  /**
   * Returns a new channel factory for the given gRPC service.
   *
   * @return the channel factory
   */
  ChannelFactory getFactory();

}
