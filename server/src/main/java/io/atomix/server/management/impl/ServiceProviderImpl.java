package io.atomix.server.management.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.google.common.base.Strings;
import io.atomix.server.NodeConfig;
import io.atomix.server.management.ChannelService;
import io.atomix.server.management.ServiceFactory;
import io.atomix.server.management.ServiceProvider;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.grpc.Channel;

/**
 * Service provider implementation.
 */
@Component
public class ServiceProviderImpl implements ServiceProvider {
  @Dependency
  private ChannelService channelService;

  @Override
  public <T> ServiceFactory<T> getFactory(Function<Channel, T> factory) {
    return new ServiceFactoryImpl<>(factory);
  }

  private class ServiceFactoryImpl<T> implements ServiceFactory<T> {
    private final Function<Channel, T> factory;
    private final Map<NodeConfig, T> services = new ConcurrentHashMap<>();

    ServiceFactoryImpl(Function<Channel, T> factory) {
      this.factory = factory;
    }

    private T getService(NodeConfig node) {
      T service = services.get(node);
      if (service == null) {
        service = services.compute(node, (id, value) -> {
          if (value == null) {
            if (!Strings.isNullOrEmpty(node.getId())) {
              value = factory.apply(channelService.getChannel(node.getHost()));
            } else {
              value = factory.apply(channelService.getChannel(node.getHost(), node.getPort()));
            }
          }
          return value;
        });
      }
      return service;
    }

    @Override
    public T getService(String target) {
      return getService(NodeConfig.newBuilder()
          .setId(target)
          .build());
    }

    @Override
    public T getService(String host, int port) {
      return getService(NodeConfig.newBuilder()
          .setHost(host)
          .setPort(port)
          .build());
    }
  }
}
