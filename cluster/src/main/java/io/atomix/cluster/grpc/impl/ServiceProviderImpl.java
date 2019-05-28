package io.atomix.cluster.grpc.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.grpc.ChannelService;
import io.atomix.cluster.grpc.ServiceFactory;
import io.atomix.cluster.grpc.ServiceProvider;
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
  @Dependency
  private ClusterMembershipService membershipService;

  @Override
  public <T> ServiceFactory<T> getFactory(Function<Channel, T> factory) {
    return new ServiceFactoryImpl<>(factory);
  }

  private class ServiceFactoryImpl<T> implements ServiceFactory<T> {
    private final Function<Channel, T> factory;
    private final Map<MemberId, T> services = new ConcurrentHashMap<>();

    ServiceFactoryImpl(Function<Channel, T> factory) {
      this.factory = factory;
    }

    @Override
    public T getService(MemberId memberId) {
      T service = services.get(memberId);
      if (service == null) {
        service = services.compute(memberId, (id, value) -> {
          if (value == null) {
            Member member = membershipService.getMember(memberId);
            value = member == null ? null
                : factory.apply(channelService.getChannel(member.getHost(), member.getPort()));
          }
          return value;
        });
      }
      return service;
    }
  }
}
