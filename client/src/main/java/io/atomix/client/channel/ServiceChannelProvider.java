package io.atomix.client.channel;

import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.NettyChannelBuilder;

/**
 * Service-based channel factory.
 */
public class ServiceChannelProvider implements ChannelProvider {
  private final String service;
  private final ChannelConfig config;

  public ServiceChannelProvider(String service, ChannelConfig config) {
    this.service = service;
    this.config = config;
  }

  @Override
  public ChannelFactory getFactory() {
    NettyChannelBuilder builder;
    if (config.isTlsEnabled()) {
      builder = NettyChannelBuilder.forTarget(service)
          .nameResolverFactory(new DnsNameResolverProvider())
          .useTransportSecurity();
    } else {
      builder = NettyChannelBuilder.forTarget(service)
          .nameResolverFactory(new DnsNameResolverProvider())
          .usePlaintext();
    }
    return builder::build;
  }
}
