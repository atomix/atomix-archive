package io.atomix.server.management.impl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.atomix.server.ServerConfig;
import io.atomix.server.management.ServiceRegistry;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Futures;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.MutableHandlerRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Service registry implementation.
 */
@Component(ServerConfig.class)
public class ServiceRegistryImpl implements ServiceRegistry, Managed<ServerConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelServiceImpl.class);
  private final MutableHandlerRegistry registry = new MutableHandlerRegistry();
  private Server server;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Class<? extends ServerChannel> serverChannelClass;

  @Override
  public void register(BindableService service) {
    registry.addService(service);
  }

  @Override
  public CompletableFuture<Void> start(ServerConfig config) {
    initEventLoopGroup();
    if (config.getTls().getEnabled()) {
      server = NettyServerBuilder.forPort(config.getNode().getPort())
          .useTransportSecurity(
              new File(config.getTls().getCertPath()),
              new File(config.getTls().getKeyPath()))
          .fallbackHandlerRegistry(registry)
          .build();
    } else {
      server = NettyServerBuilder.forPort(config.getNode().getPort())
          .fallbackHandlerRegistry(registry)
          .channelType(serverChannelClass)
          .bossEventLoopGroup(bossGroup)
          .workerEventLoopGroup(workerGroup)
          .build();
    }
    try {
      server.start();
    } catch (IOException e) {
      return Futures.exceptionalFuture(e);
    }
    return CompletableFuture.completedFuture(null);
  }

  private void initEventLoopGroup() {
    // try Epoll first and if that does work, use nio.
    try {
      workerGroup = new EpollEventLoopGroup(0, namedThreads("netty-messaging-event-epoll-client-%d", LOGGER));
      bossGroup = new EpollEventLoopGroup(0, namedThreads("netty-messaging-event-epoll-server-%d", LOGGER));
      serverChannelClass = EpollServerSocketChannel.class;
      return;
    } catch (Throwable e) {
      LOGGER.debug("Failed to initialize native (epoll) transport. "
          + "Reason: {}. Proceeding with nio.", e.getMessage());
    }
    workerGroup = new NioEventLoopGroup(0, namedThreads("netty-messaging-event-nio-client-%d", LOGGER));
    bossGroup = new NioEventLoopGroup(0, namedThreads("netty-messaging-event-nio-server-%d", LOGGER));
    serverChannelClass = NioServerSocketChannel.class;
  }


  @Override
  public CompletableFuture<Void> stop() {
    if (server != null) {
      server.shutdownNow();
    }
    return CompletableFuture.completedFuture(null);
  }
}
