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
package io.atomix.protocols.log;

import io.atomix.primitive.protocol.LogProtocol;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.component.Component;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Distributed log protocol.
 */
public class DistributedLogProtocol implements LogProtocol<DistributedLog> {
  public static final Type TYPE = new Type();

  /**
   * Log protocol type.
   */
  @Component
  public static final class Type implements PrimitiveProtocol.Type<DistributedLog> {
    private static final String NAME = "multi-log";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public DistributedLog newConfig() {
      return DistributedLog.newBuilder().build();
    }

    @Override
    public PrimitiveProtocol newProtocol(DistributedLog config) {
      return new DistributedLogProtocol(config);
    }
  }

  private final DistributedLog config;

  protected DistributedLogProtocol(DistributedLog config) {
    this.config = checkNotNull(config, "config cannot be null");
  }

  @Override
  public PrimitiveProtocol.Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return config.getGroup();
  }

  /**
   * Returns the protocol configuration.
   *
   * @return the protocol configuration
   */
  public DistributedLog config() {
    return config;
  }

  @Override
  public DistributedLog toProto() {
    return DistributedLog.newBuilder()
        .setGroup(group())
        .setPartitions(config.getPartitions())
        .setReplicationFactor(config.getReplicationFactor())
        .setReplicationStrategy(config.getReplicationStrategy())
        .build();
  }
}
