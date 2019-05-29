package io.atomix.server.management.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.server.TlsConfig;
import io.atomix.server.management.ChannelService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import io.grpc.Channel;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * gRPC service registry.
 */
@Component(TlsConfig.class)
public class ChannelServiceImpl implements ChannelService, Managed<TlsConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelServiceImpl.class);
  private TlsConfig config;
  private EventLoopGroup clientGroup;
  private Class<? extends io.netty.channel.Channel> clientChannelClass;

  @Override
  public Channel getChannel(String target) {
    NettyChannelBuilder builder;
    if (config.getEnabled()) {
      builder = NettyChannelBuilder.forTarget(target)
          .nameResolverFactory(new DnsNameResolverProvider())
          .channelType(clientChannelClass)
          .eventLoopGroup(clientGroup)
          .useTransportSecurity();
    } else {
      builder = NettyChannelBuilder.forTarget(target)
          .nameResolverFactory(new DnsNameResolverProvider())
          .channelType(clientChannelClass)
          .eventLoopGroup(clientGroup)
          .usePlaintext();
    }
    return builder.build();
  }

  @Override
  public Channel getChannel(String host, int port) {
    NettyChannelBuilder builder;
    if (config.getEnabled()) {
      builder = NettyChannelBuilder.forAddress(host, port)
          .channelType(clientChannelClass)
          .eventLoopGroup(clientGroup)
          .useTransportSecurity();
    } else {
      builder = NettyChannelBuilder.forAddress(host, port)
          .channelType(clientChannelClass)
          .eventLoopGroup(clientGroup)
          .usePlaintext();
    }
    return builder.build();
  }

  @Override
  public CompletableFuture<Void> start(TlsConfig config) {
    this.config = config;
    initEventLoopGroup();
    return CompletableFuture.completedFuture(null);
  }

  private void initEventLoopGroup() {
    // try Epoll first and if that does work, use nio.
    try {
      clientGroup = new EpollEventLoopGroup(0, namedThreads("netty-messaging-event-epoll-client-%d", LOGGER));
      clientChannelClass = EpollSocketChannel.class;
      return;
    } catch (Throwable e) {
      LOGGER.debug("Failed to initialize native (epoll) transport. "
          + "Reason: {}. Proceeding with nio.", e.getMessage());
    }
    clientGroup = new NioEventLoopGroup(0, namedThreads("netty-messaging-event-nio-client-%d", LOGGER));
    clientChannelClass = NioSocketChannel.class;
  }
}
