package io.atomix.cluster.impl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.GrpcService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Futures;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.util.MutableHandlerRegistry;

/**
 * gRPC service registry.
 */
@Component(ClusterConfig.class)
public class GrpcServiceImpl implements GrpcService, Managed<ClusterConfig> {
  private final MutableHandlerRegistry registry = new MutableHandlerRegistry();
  private ClusterConfig config;
  private Server server;

  @Override
  public void register(BindableService service) {
    registry.addService(service);
  }

  @Override
  public Channel getChannel(String host, int port) {
    ManagedChannelBuilder builder;
    if (config.getMessagingConfig().getTlsConfig().isEnabled()) {
      builder = ManagedChannelBuilder.forAddress(host, port).useTransportSecurity();
    } else {
      builder = ManagedChannelBuilder.forAddress(host, port).usePlaintext();
    }
    return builder.build();
  }

  @Override
  public CompletableFuture<Void> start(ClusterConfig config) {
    this.config = config;
    if (config.getMessagingConfig().getTlsConfig().isEnabled()) {
      server = ServerBuilder.forPort(config.getNodeConfig().getPort())
          .useTransportSecurity(
              new File(config.getMessagingConfig().getTlsConfig().getCertPath()),
              new File(config.getMessagingConfig().getTlsConfig().getKeyPath()))
          .fallbackHandlerRegistry(registry)
          .build();
    } else {
      server = ServerBuilder.forPort(config.getNodeConfig().getPort())
          .fallbackHandlerRegistry(registry)
          .build();
    }
    try {
      server.start();
    } catch (IOException e) {
      return Futures.exceptionalFuture(e);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (server != null) {
      server.shutdownNow();
    }
    return CompletableFuture.completedFuture(null);
  }
}
