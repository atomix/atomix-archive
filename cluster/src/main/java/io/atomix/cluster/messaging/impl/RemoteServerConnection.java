/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

import java.util.function.BiConsumer;

import io.netty.channel.Channel;

/**
 * Remote server connection manages messaging on the server side of a Netty connection.
 */
final class RemoteServerConnection extends AbstractServerConnection {
  private final Channel channel;

  RemoteServerConnection(HandlerRegistry<String, BiConsumer<ProtocolMessage, ServerConnection>> handlers, Channel channel) {
    super(handlers);
    this.channel = channel;
  }

  @Override
  public void reply(ProtocolMessage message) {
    channel.writeAndFlush(message, channel.voidPromise());
  }
}
