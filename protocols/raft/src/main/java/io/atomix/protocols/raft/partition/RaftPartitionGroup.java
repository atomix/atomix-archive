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
package io.atomix.protocols.raft.partition;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.impl.DefaultPrimitiveClient;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadata;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.partition.RaftPartitionGroupConfig;
import io.atomix.primitive.partition.ServiceProvider;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.protocols.raft.MultiRaft;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.RaftPartitionGroupMetadata;
import io.atomix.protocols.raft.RaftPrimitiveMetadata;
import io.atomix.protocols.raft.partition.impl.RaftProtocolManager;
import io.atomix.raft.RaftClient;
import io.atomix.raft.impl.DefaultRaftClient;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Raft partition group.
 */
public class RaftPartitionGroup implements ManagedPartitionGroup, ServiceProvider<MultiRaft> {
  public static final Type TYPE = new Type();

  /**
   * Raft partition group type.
   */
  @Component
  public static class Type implements PartitionGroup.Type {
    private static final String NAME = "raft";

    @Dependency
    private RaftProtocolManager raftProtocolManager;

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Class<?> getConfigType() {
      return RaftPartitionGroupConfig.class;
    }

    @Override
    public Descriptors.Descriptor getConfigDescriptor() {
      return RaftPartitionGroupConfig.getDescriptor();
    }

    @Override
    public Class<?> getProtocolType() {
      return MultiRaft.class;
    }

    @Override
    public Descriptors.Descriptor getProtocolDescriptor() {
      return MultiRaft.getDescriptor();
    }

    @Override
    public PartitionGroupConfig newConfig() {
      return PartitionGroupConfig.newBuilder()
          .setRaft(RaftPartitionGroupConfig.newBuilder().build())
          .build();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(PartitionGroupConfig config) {
      return new RaftPartitionGroup(raftProtocolManager, config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftPartitionGroup.class);

  private static Collection<RaftPartition> buildPartitions(
      PartitionGroupConfig config,
      RaftProtocolManager raftProtocolManager,
      ThreadContextFactory threadContextFactory) {
    File partitionsDir = new File(new File(config.getRaft().getStorage().getDirectory(), config.getName()), "partitions");
    List<RaftPartition> partitions = new ArrayList<>(config.getRaft().getPartitions());
    for (int i = 0; i < config.getRaft().getPartitions(); i++) {
      partitions.add(new RaftPartition(
          PartitionId.newBuilder()
              .setGroup(config.getName())
              .setPartition(i + 1)
              .build(),
          config,
          raftProtocolManager,
          new File(partitionsDir, String.valueOf(i + 1)),
          threadContextFactory));
    }
    return partitions;
  }

  private static final Duration SNAPSHOT_TIMEOUT = Duration.ofSeconds(15);

  private final RaftProtocolManager raftProtocolManager;
  private final String name;
  private final PartitionGroupConfig config;
  private final int partitionSize;
  private final ThreadContextFactory threadContextFactory;
  private final Map<PartitionId, RaftPartition> partitions = Maps.newConcurrentMap();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private Collection<PartitionMetadata> metadata;
  private PartitionManagementService managementService;
  private final String snapshotSubject;

  public RaftPartitionGroup(RaftProtocolManager raftProtocolManager, PartitionGroupConfig config) {
    Logger log = ContextualLoggerFactory.getLogger(DefaultRaftClient.class, LoggerContext.builder(RaftClient.class)
        .addValue(config.getName())
        .build());
    this.raftProtocolManager = raftProtocolManager;
    this.name = config.getName();
    this.config = config;
    this.partitionSize = config.getRaft().getPartitionSize();

    int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory(
        "raft-partition-group-" + name + "-%d", threadPoolSize, log);
    this.snapshotSubject = "raft-partition-group-" + name + "-snapshot";

    buildPartitions(config, raftProtocolManager, threadContextFactory).forEach(p -> {
      this.partitions.put(p.id(), p);
      this.sortedPartitionIds.add(p.id());
    });
    Collections.sort(sortedPartitionIds, Comparator.comparingInt(PartitionId::getPartition));
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
  public ServiceProtocol newProtocol() {
    return new MultiRaftProtocol(MultiRaft.newBuilder()
        .setGroup(name)
        .build());
  }

  @Override
  public RaftPartition getPartition(PartitionId partitionId) {
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
    this.metadata = buildPartitions();
    this.managementService = managementService;
    List<CompletableFuture<Partition>> futures = metadata.stream()
        .map(metadata -> {
          RaftPartition partition = partitions.get(metadata.id());
          return partition.open(metadata, managementService);
        })
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    return join(managementService);
  }

  @Override
  public CompletableFuture<PrimitiveClient> createService(String name, MultiRaft protocol) {
    return createPrimitive(RaftPrimitiveMetadata.newBuilder()
        .setName(name)
        .build())
        .thenApply(metadata -> {
          Map<PartitionId, PartitionClient> partitions = getPartitions().stream()
              .map(partition -> Maps.immutableEntry(partition.id(), partition.getClient()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          return new DefaultPrimitiveClient(partitions, Partitioner.MURMUR3);
        });
  }

  /**
   * Creates a new Raft primitive.
   *
   * @param metadata the primitive metadata
   * @return a future to be completed once the primitive has been created
   */
  private CompletableFuture<RaftPrimitiveMetadata> createPrimitive(RaftPrimitiveMetadata metadata) {
    return managementService.getMetadataService().update(
        name,
        RaftPartitionGroupMetadata::parseFrom,
        groupMetadata -> {
          if (groupMetadata == null) {
            groupMetadata = RaftPartitionGroupMetadata.newBuilder().build();
          }

          RaftPrimitiveMetadata primitive = groupMetadata.getPrimitivesMap().get(metadata.getName());
          if (primitive == null) {
            groupMetadata = RaftPartitionGroupMetadata.newBuilder(groupMetadata)
                .putPrimitives(metadata.getName(), metadata)
                .build();
          }
          return groupMetadata;
        },
        RaftPartitionGroupMetadata::toByteArray)
        .thenApply(groupMetadata -> groupMetadata.getPrimitivesMap().get(metadata.getName()));
  }

  private Collection<PartitionMetadata> buildPartitions() {
    List<MemberId> sorted = new ArrayList<>(config.getRaft().getMembersList().stream()
        .map(MemberId::from)
        .collect(Collectors.toSet()));
    Collections.sort(sorted);

    int partitionSize = this.partitionSize;
    if (partitionSize == 0) {
      partitionSize = sorted.size();
    }

    int length = sorted.size();
    int count = Math.min(partitionSize, length);

    Set<PartitionMetadata> metadata = Sets.newHashSet();
    for (int i = 0; i < partitions.size(); i++) {
      PartitionId partitionId = sortedPartitionIds.get(i);
      Set<MemberId> set = new HashSet<>(count);
      for (int j = 0; j < count; j++) {
        set.add(sorted.get((i + j) % length));
      }
      metadata.add(new PartitionMetadata(partitionId, set));
    }
    return metadata;
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = partitions.values().stream()
        .map(RaftPartition::close)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
      threadContextFactory.close();
      LOGGER.info("Stopped");
    });
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("partitions", partitions)
        .toString();
  }
}
