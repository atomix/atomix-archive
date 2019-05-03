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
import io.atomix.utils.component.Component;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;

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
  @Component(scope = Component.Scope.TEST)
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

  private final String name;
  private final TestPartitionGroupConfig config;
  private final Map<PartitionId, TestPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();

  public TestPartitionGroup(TestPartitionGroupConfig config) {
    this.name = config.getName();
    this.config = config;

    buildPartitions(config).forEach(p -> {
      this.partitions.put(p.id(), p);
      this.sortedPartitionIds.add(p.id());
    });
    Collections.sort(sortedPartitionIds, Comparator.comparingInt(PartitionId::getPartition));
  }

  private static Collection<TestPartition> buildPartitions(TestPartitionGroupConfig config) {
    List<TestPartition> partitions = new ArrayList<>(config.getPartitions());
    for (int i = 0; i < config.getPartitions(); i++) {
      partitions.add(new TestPartition(
          PartitionId.newBuilder()
              .setGroup(config.getName())
              .setPartition(i + 1)
              .build()));
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
    return Futures.allOf(partitions.values().stream().map(partition -> partition.join(managementService)))
        .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    return join(managementService);
  }

  @Override
  public CompletableFuture<Void> close() {
    return Futures.allOf(partitions.values().stream().map(partition -> partition.close()))
        .thenApply(v -> null);
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

    public Builder withNumPartitions(int numPartitions) {
      config.setPartitions(numPartitions);
      return this;
    }

    @Override
    public TestPartitionGroup build() {
      return new TestPartitionGroup(config);
    }
  }
}
