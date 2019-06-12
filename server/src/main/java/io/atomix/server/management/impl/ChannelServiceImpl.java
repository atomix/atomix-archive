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
package io.atomix.server.management.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.server.management.ChannelService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import io.grpc.Channel;
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
@Component
public class ChannelServiceImpl implements ChannelService, Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelServiceImpl.class);
  private EventLoopGroup clientGroup;
  private Class<? extends io.netty.channel.Channel> clientChannelClass;

  @Override
  public Channel getChannel(String host, int port) {
    return NettyChannelBuilder.forAddress(host, port)
        .channelType(clientChannelClass)
        .eventLoopGroup(clientGroup)
        .usePlaintext()
        .build();
  }

  @Override
  public CompletableFuture<Void> start() {
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
