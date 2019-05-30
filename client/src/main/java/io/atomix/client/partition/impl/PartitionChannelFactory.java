package io.atomix.client.partition.impl;

import io.atomix.api.partition.Partition;
import io.atomix.api.partition.PartitionEndpoint;
import io.atomix.client.channel.ChannelFactory;
import io.grpc.Channel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

/**
 * Partition group channel factory.
 */
public class PartitionChannelFactory implements ChannelFactory {
  private final Partition partition;
  private final NettyChannelBuilder builder;

  public PartitionChannelFactory(Partition partition) {
    this.partition = partition;
    PartitionEndpoint endpoint = partition.getEndpoints(0);
    if (endpoint.getPort() != 0) {
      this.builder = NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext();
    } else {
      this.builder = NettyChannelBuilder.forTarget(endpoint.getHost()).usePlaintext();
    }
  }

  @Override
  public Channel getChannel() {
    return builder.build();
  }
}
