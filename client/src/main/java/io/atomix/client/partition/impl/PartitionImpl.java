package io.atomix.client.partition.impl;

import io.atomix.client.channel.ChannelFactory;
import io.atomix.client.partition.Partition;

/**
 * Partition implementation.
 */
public class PartitionImpl implements Partition {
  private final io.atomix.api.partition.Partition partition;
  private final ChannelFactory channelFactory;

  public PartitionImpl(io.atomix.api.partition.Partition partition) {
    this.partition = partition;
    this.channelFactory = new PartitionChannelFactory(partition);
  }

  @Override
  public int id() {
    return partition.getPartitionId();
  }

  @Override
  public ChannelFactory getChannelFactory() {
    return channelFactory;
  }
}
