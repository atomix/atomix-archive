package io.atomix.client.channel;

import io.atomix.utils.net.Address;
import io.grpc.netty.NettyChannelBuilder;

/**
 * Server channel provider.
 */
public class ServerChannelProvider implements ChannelProvider {
  private final Address address;
  private final ChannelConfig config;

  public ServerChannelProvider(Address address, ChannelConfig config) {
    this.address = address;
    this.config = config;
  }

  @Override
  public ChannelFactory getFactory() {
    NettyChannelBuilder builder;
    if (config.isTlsEnabled()) {
      builder = NettyChannelBuilder.forAddress(address.host(), address.port())
          .useTransportSecurity();
    } else {
      builder = NettyChannelBuilder.forAddress(address.host(), address.port())
          .usePlaintext();
    }
    return builder::build;
  }
}
