/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.core.value.impl;

import java.util.concurrent.CompletableFuture;

import com.google.common.io.BaseEncoding;
import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.core.value.AtomicValueConfig;
import io.atomix.core.value.ValueId;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.DistributedLogProtocol;
import io.atomix.primitive.protocol.MultiRaftProtocol;
import io.atomix.utils.serializer.Serializer;

/**
 * Default implementation of AtomicValueBuilder.
 *
 * @param <V> value type
 */
public class DefaultAtomicValueBuilder<V> extends AtomicValueBuilder<V> {
  public DefaultAtomicValueBuilder(String name, AtomicValueConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  private ValueId createValueId() {
    ValueId.Builder builder = ValueId.newBuilder().setName(name);
    protocol = protocol();
    if (protocol instanceof io.atomix.protocols.raft.MultiRaftProtocol) {
      builder.setRaft(MultiRaftProtocol.newBuilder()
          .setGroup(((io.atomix.protocols.raft.MultiRaftProtocol) protocol).group())
          .build());
    } else if (protocol instanceof io.atomix.protocols.log.DistributedLogProtocol) {
      builder.setLog(DistributedLogProtocol.newBuilder()
          .setGroup(((io.atomix.protocols.log.DistributedLogProtocol) protocol).group())
          .setPartitions(((io.atomix.protocols.log.DistributedLogProtocol) protocol).config().getPartitions())
          .setReplicationFactor(((io.atomix.protocols.log.DistributedLogProtocol) protocol).config().getReplicationFactor())
          .build());
    }
    return builder.build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicValue<V>> buildAsync() {
    return new DefaultAsyncAtomicValue(createValueId(), getChannelFactory(), managementService, Partitioner.MURMUR3, config.getSessionTimeout())
        .connect()
        .thenApply(rawValue -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicValue<V, String>(
              rawValue,
              value -> BaseEncoding.base16().encode(serializer.encode(value)),
              string -> serializer.decode(BaseEncoding.base16().decode(string)));
        })
        .thenApply(AsyncAtomicValue::sync);
  }
}
