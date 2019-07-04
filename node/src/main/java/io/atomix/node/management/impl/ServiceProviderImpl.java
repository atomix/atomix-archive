/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.node.management.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.atomix.node.management.ChannelService;
import io.atomix.node.management.ClusterService;
import io.atomix.node.management.Node;
import io.atomix.node.management.ServiceFactory;
import io.atomix.node.management.ServiceProvider;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.grpc.Channel;

/**
 * Service provider implementation.
 */
@Component
public class ServiceProviderImpl implements ServiceProvider {
  @Dependency
  private ClusterService clusterService;
  @Dependency
  private ChannelService channelService;

  @Override
  public <T> ServiceFactory<T> getFactory(Function<Channel, T> factory) {
    return new ServiceFactoryImpl<>(factory);
  }

  private class ServiceFactoryImpl<T> implements ServiceFactory<T> {
    private final Function<Channel, T> factory;
    private final Map<Node, T> services = new ConcurrentHashMap<>();

    ServiceFactoryImpl(Function<Channel, T> factory) {
      this.factory = factory;
    }

    private T getService(Node node) {
      T service = services.get(node);
      if (service == null) {
        service = services.compute(node, (id, value) -> {
          if (value == null) {
            value = factory.apply(channelService.getChannel(node.host(), node.port()));
          }
          return value;
        });
      }
      return service;
    }

    @Override
    public T getService(String nodeId) {
      return getService(clusterService.getNode(nodeId));
    }

    @Override
    public T getService(String host, int port) {
      return getService(new Node(host, host, port));
    }
  }
}
