package io.atomix.client.partition;

import io.atomix.client.channel.ChannelFactory;

/**
 * Partition.
 */
public interface Partition {

  /**
   * Returns the partition ID.
   *
   * @return the partition ID
   */
  int id();

  /**
   * Returns the channel factory.
   *
   * @return the channel factory
   */
  ChannelFactory getChannelFactory();

}
