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
package io.atomix.core.test.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.raft.RaftClient;
import io.atomix.raft.impl.DefaultRaftClient;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Raft partition group.
 */
public class TestPartitionGroup implements ManagedPartitionGroup {
  public static final Type TYPE = new Type();

  /**
   * Returns a new Raft partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(new TestPartitionGroupConfig().setName(name));
  }

  /**
   * Raft partition group type.
   */
  public static class Type implements PartitionGroup.Type<TestPartitionGroupConfig> {
    private static final String NAME = "raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Namespace namespace() {
      return Namespaces.BASIC;
    }

    @Override
    public TestPartitionGroupConfig newConfig() {
      return new TestPartitionGroupConfig();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(TestPartitionGroupConfig config) {
      return new TestPartitionGroup(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TestPartitionGroup.class);

  private final String name;
  private final TestPartitionGroupConfig config;
  private final ThreadContextFactory threadContextFactory;
  private final Map<PartitionId, TestPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();

  public TestPartitionGroup(TestPartitionGroupConfig config) {
    Logger log = ContextualLoggerFactory.getLogger(DefaultRaftClient.class, LoggerContext.builder(RaftClient.class)
        .addValue(config.getName())
        .build());
    this.name = config.getName();
    this.config = config;
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory(
        "raft-partition-group-" + name + "-%d", 4, log);

    buildPartitions(config, threadContextFactory).forEach(p -> {
      this.partitions.put(p.id(), p);
      this.sortedPartitionIds.add(p.id());
    });
    Collections.sort(sortedPartitionIds, Comparator.comparingInt(PartitionId::getPartition));
  }

  private static Collection<TestPartition> buildPartitions(
      TestPartitionGroupConfig config,
      ThreadContextFactory threadContextFactory) {
    List<TestPartition> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 0; i < config.getPartitions(); i++) {
      partitions.add(new TestPartition(
          PartitionId.newBuilder()
              .setGroup(config.getName())
              .setPartition(i + 1)
              .build(), threadContextFactory));
    }
    return partitions;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PartitionGroup.Type type() {
    return TYPE;
  }

  @Override
  public PrimitiveProtocol.Type protocol() {
    return MultiRaftProtocol.TYPE;
  }

  @Override
  public PartitionGroupConfig config() {
    return config;
  }

  @Override
  public ProxyProtocol newProtocol() {
    return MultiRaftProtocol.builder(name)
        .withRecoveryStrategy(Recovery.RECOVER)
        .withMaxRetries(5)
        .build();
  }

  @Override
  public TestPartition getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition> getPartitions() {
    return (Collection) partitions.values();
  }

  @Override
  public List<PartitionId> getPartitionIds() {
    return sortedPartitionIds;
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    return join(managementService);
  }

  @Override
  public CompletableFuture<Void> close() {
    threadContextFactory.close();
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("partitions", partitions)
        .toString();
  }

  /**
   * Raft partition group builder.
   */
  public static class Builder extends PartitionGroup.Builder<TestPartitionGroupConfig> {
    protected Builder(TestPartitionGroupConfig config) {
      super(config);
    }

    @Override
    public TestPartitionGroup build() {
      return new TestPartitionGroup(config);
    }
  }
}
