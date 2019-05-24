/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.client.impl;

import io.atomix.client.PrimitiveCache;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.channel.ChannelFactory;
import io.atomix.utils.concurrent.ThreadContextFactory;

/**
 * Default primitive management service.
 */
public class DefaultPrimitiveManagementService implements PrimitiveManagementService {
  private final PrimitiveCache primitiveCache;
  private final ChannelFactory channelFactory;
  private final ThreadContextFactory threadContextFactory;

  public DefaultPrimitiveManagementService(
      PrimitiveCache primitiveCache,
      ChannelFactory channelFactory,
      ThreadContextFactory threadContextFactory) {
    this.primitiveCache = primitiveCache;
    this.channelFactory = channelFactory;
    this.threadContextFactory = threadContextFactory;
  }

  @Override
  public PrimitiveCache getPrimitiveCache() {
    return primitiveCache;
  }

  @Override
  public ThreadContextFactory getThreadFactory() {
    return threadContextFactory;
  }

  @Override
  public ChannelFactory getChannelFactory() {
    return channelFactory;
  }
}
