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
import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.utils.component.Component;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-Raft protocol.
 */
public class MultiRaftProtocol implements ServiceProtocol<MultiRaft> {
  public static final Type TYPE = new Type();

  /**
   * Multi-Raft protocol type.
   */
  @Component
  public static final class Type implements PrimitiveProtocol.Type<MultiRaft> {
    private static final String NAME = "multi-raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public MultiRaft newConfig() {
      return MultiRaft.newBuilder().build();
    }

    @Override
    public PrimitiveProtocol newProtocol(MultiRaft config) {
      return new MultiRaftProtocol(config);
    }
  }

  private final MultiRaft config;

  public MultiRaftProtocol(MultiRaft config) {
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

  @Override
  public MultiRaft toProto() {
    return MultiRaft.newBuilder()
        .setGroup(group())
        .build();
  }
}
