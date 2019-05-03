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
package io.atomix.protocols.raft;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.component.Component;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-Raft protocol.
 */
public class MultiRaftProtocol implements ProxyProtocol {
  public static final Type TYPE = new Type();

  /**
   * Returns an instance of the multi-Raft protocol with the default configuration.
   *
   * @return an instance of the multi-Raft protocol with the default configuration
   */
  public static MultiRaftProtocol instance() {
    return new MultiRaftProtocol(new MultiRaftProtocolConfig());
  }

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @return a new multi-Raft protocol builder
   */
  public static MultiRaftProtocolBuilder builder() {
    return new MultiRaftProtocolBuilder(new MultiRaftProtocolConfig());
  }

  /**
   * Returns a new multi-Raft protocol builder.
   *
   * @param group the partition group
   * @return the multi-Raft protocol builder
   */
  public static MultiRaftProtocolBuilder builder(String group) {
    return new MultiRaftProtocolBuilder(new MultiRaftProtocolConfig().setGroup(group));
  }

  /**
   * Multi-Raft protocol type.
   */
  @Component
  public static final class Type implements PrimitiveProtocol.Type<MultiRaftProtocolConfig> {
    private static final String NAME = "multi-raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public MultiRaftProtocolConfig newConfig() {
      return new MultiRaftProtocolConfig();
    }

    @Override
    public PrimitiveProtocol newProtocol(MultiRaftProtocolConfig config) {
      return new MultiRaftProtocol(config);
    }
  }

  private final MultiRaftProtocolConfig config;

  protected MultiRaftProtocol(MultiRaftProtocolConfig config) {
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
}
