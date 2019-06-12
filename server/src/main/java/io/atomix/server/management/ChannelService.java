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
package io.atomix.server.management;

import io.grpc.Channel;

/**
 * Service registry.
 */
public interface ChannelService {

  /**
   * Gets a channel for the given node.
   *
   * @param host the node host
   * @param port the node port
   * @return the channel
   */
  Channel getChannel(String host, int port);

}
