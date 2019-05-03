/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.cluster.messaging.impl;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.utils.AtomixRuntimeException;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Netty broadcast service.
 */
@Component(ClusterConfig.class)
public class NettyBroadcastService implements BroadcastService, Managed<ClusterConfig> {
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
      .register(Message.class)
      .build());

  private final Logger log = LoggerFactory.getLogger(getClass());

  private boolean enabled;
  private InetSocketAddress localAddress;
  private InetSocketAddress groupAddress;
  private NetworkInterface iface;
  private EventLoopGroup group;
  private Channel serverChannel;
  private DatagramChannel clientChannel;

  private final Map<String, Set<Consumer<byte[]>>> listeners = Maps.newConcurrentMap();

  @Override
  public void broadcast(String subject, byte[] payload) {
    if (enabled) {
      Message message = new Message(subject, payload);
      byte[] bytes = SERIALIZER.encode(message);
      ByteBuf buf = serverChannel.alloc().buffer(4 + bytes.length);
      buf.writeInt(bytes.length).writeBytes(bytes);
      serverChannel.writeAndFlush(new DatagramPacket(buf, groupAddress));
    }
  }

  @Override
  public synchronized void addListener(String subject, Consumer<byte[]> listener) {
    listeners.computeIfAbsent(subject, s -> Sets.newCopyOnWriteArraySet()).add(listener);
  }

  @Override
  public synchronized void removeListener(String subject, Consumer<byte[]> listener) {
    Set<Consumer<byte[]>> listeners = this.listeners.get(subject);
    if (listeners != null) {
      listeners.remove(listener);
      if (listeners.isEmpty()) {
        this.listeners.remove(subject);
      }
    }
  }

  private CompletableFuture<Void> bootstrapServer() {
    Bootstrap serverBootstrap = new Bootstrap()
        .group(group)
        .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
        .handler(new SimpleChannelInboundHandler<Object>() {
          @Override
          public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            // Nothing will be sent.
          }
        })
        .option(ChannelOption.IP_MULTICAST_IF, iface)
        .option(ChannelOption.SO_REUSEADDR, true);

    CompletableFuture<Void> future = new CompletableFuture<>();
    serverBootstrap.bind(localAddress).addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        serverChannel = f.channel();
        future.complete(null);
      } else {
        future.completeExceptionally(f.cause());
      }
    });
    return future;
  }

  private CompletableFuture<Void> bootstrapClient() {
    Bootstrap clientBootstrap = new Bootstrap()
        .group(group)
        .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
        .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
          @Override
          protected void channelRead0(ChannelHandlerContext context, DatagramPacket packet) throws Exception {
            byte[] payload = new byte[packet.content().readInt()];
            packet.content().readBytes(payload);
            Message message = SERIALIZER.decode(payload);
            Set<Consumer<byte[]>> listeners = NettyBroadcastService.this.listeners.get(message.subject());
            if (listeners != null) {
              for (Consumer<byte[]> listener : listeners) {
                listener.accept(message.payload());
              }
            }
          }
        })
        .option(ChannelOption.IP_MULTICAST_IF, iface)
        .option(ChannelOption.SO_REUSEADDR, true)
        .localAddress(localAddress.getPort());

    CompletableFuture<Void> future = new CompletableFuture<>();
    clientBootstrap.bind().addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        clientChannel = (DatagramChannel) f.channel();
        log.info("{} joining multicast group {} on port {}", localAddress.getHostName(), groupAddress.getHostName(), groupAddress.getPort());
        clientChannel.joinGroup(groupAddress, iface).addListener(f2 -> {
          if (f2.isSuccess()) {
            log.info("{} successfully joined multicast group {} on port {}", localAddress.getHostName(), groupAddress.getHostName(), groupAddress.getPort());
            future.complete(null);
          } else {
            log.info("{} failed to join group {} on port {}", localAddress.getHostName(), groupAddress.getHostName(), groupAddress.getPort());
            future.completeExceptionally(f2.cause());
          }
        });
      } else {
        future.completeExceptionally(f.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> start(ClusterConfig config) {
    this.enabled = config.getMulticastConfig().isEnabled();
    // intentionally using the multicast port for localAddress
    this.localAddress = new InetSocketAddress(config.getNodeConfig().getAddress().host(), config.getNodeConfig().getAddress().port());
    this.groupAddress = new InetSocketAddress(config.getMulticastConfig().getGroup().getHostAddress(), config.getMulticastConfig().getPort());
    try {
      iface = NetworkInterface.getByInetAddress(config.getNodeConfig().getAddress().address());
    } catch (SocketException e) {
      throw new AtomixRuntimeException(e);
    }

    if (!enabled) {
      return CompletableFuture.completedFuture(null);
    }
    group = new NioEventLoopGroup(0, namedThreads("netty-broadcast-event-nio-client-%d", log));
    return bootstrapServer()
        .thenCompose(v -> bootstrapClient())
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (!enabled) {
      return CompletableFuture.completedFuture(null);
    }
    if (clientChannel != null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      clientChannel.leaveGroup(groupAddress, iface).addListener(f -> {
        group.shutdownGracefully();
        future.complete(null);
      });
      return future;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Internal broadcast service message.
   */
  static class Message {
    private final String subject;
    private final byte[] payload;

    Message() {
      this(null, null);
    }

    Message(String subject, byte[] payload) {
      this.subject = subject;
      this.payload = payload;
    }

    String subject() {
      return subject;
    }

    byte[] payload() {
      return payload;
    }
  }
}
