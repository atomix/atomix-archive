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
package io.atomix.core.lock.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AtomicLock;
import io.atomix.core.lock.AtomicLockBuilder;
import io.atomix.core.lock.AtomicLockConfig;
import io.atomix.core.lock.LockId;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.DistributedLogProtocol;
import io.atomix.primitive.protocol.MultiRaftProtocol;

/**
 * Default distributed lock builder implementation.
 */
public class DefaultAtomicLockBuilder extends AtomicLockBuilder {
  public DefaultAtomicLockBuilder(String name, AtomicLockConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  private LockId createLockId() {
    LockId.Builder builder = LockId.newBuilder().setName(name);
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
  public CompletableFuture<AtomicLock> buildAsync() {
    return new DefaultAsyncAtomicLock(createLockId(), getChannelFactory(), managementService, Partitioner.MURMUR3, config.getSessionTimeout())
        .connect()
        .thenApply(AsyncAtomicLock::sync);
  }
}
