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
package io.atomix.protocols.log.partition;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.cluster.MemberId;
import io.atomix.log.protocol.LogPartitionGroupMetadata;
import io.atomix.log.protocol.LogTopicMetadata;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadataEvent;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.log.impl.DistributedLogClient;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.component.Component;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.memory.MemorySize;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log partition group.
 */
public class LogPartitionGroup implements ManagedPartitionGroup {
  public static final Type TYPE = new Type();

  /**
   * Returns a new log partition group builder.
   *
   * @param name the partition group name
   * @return a new partition group builder
   */
  public static Builder builder(String name) {
    return new Builder(new LogPartitionGroupConfig().setName(name));
  }

  /**
   * Log partition group type.
   */
  @Component
  public static class Type implements PartitionGroup.Type<LogPartitionGroupConfig> {
    private static final String NAME = "log";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Namespace namespace() {
      return Namespace.builder()
          .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 300)
          .register(LogPartitionGroupConfig.class)
          .register(LogStorageConfig.class)
          .register(LogCompactionConfig.class)
          .register(MemorySize.class)
          .register(StorageLevel.class)
          .build();
    }

    @Override
    public LogPartitionGroupConfig newConfig() {
      return new LogPartitionGroupConfig();
    }

    @Override
    public ManagedPartitionGroup newPartitionGroup(LogPartitionGroupConfig config) {
      return new LogPartitionGroup(config);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LogPartitionGroup.class);

  private final String name;
  private final LogPartitionGroupConfig config;
  private final Map<String, Map<PartitionId, LogPartition>> topics = Maps.newConcurrentMap();
  private final Map<PartitionId, LogPartition> partitions = Maps.newConcurrentMap();
  private final List<LogPartition> sortedPartitions = Lists.newCopyOnWriteArrayList();
  private final List<PartitionId> sortedPartitionIds = Lists.newCopyOnWriteArrayList();
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;
  private final Consumer<PartitionMetadataEvent<LogPartitionGroupMetadata>> metadataListener;
  private volatile PartitionManagementService managementService;
  private final Map<String, CompletableFuture<LogTopicMetadata>> topicFutures = new ConcurrentHashMap<>();

  public LogPartitionGroup(LogPartitionGroupConfig config) {
    Logger log = ContextualLoggerFactory.getLogger(DistributedLogClient.class, LoggerContext.builder(DistributedLogClient.class)
        .addValue(config.getName())
        .build());
    this.config = config;
    this.name = checkNotNull(config.getName());
    int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory(
        "raft-partition-group-" + name + "-%d", threadPoolSize, log);
    this.threadContext = threadContextFactory.createContext();
    this.metadataListener = e -> threadContext.execute(() -> onMetadataEvent(e));
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
    return DistributedLogProtocol.TYPE;
  }

  @Override
  public PartitionGroupConfig config() {
    return config;
  }

  @Override
  public ServiceProtocol newProtocol() {
    return DistributedLogProtocol.builder(name).build();
  }

  @Override
  public LogPartition getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Partition> getPartitions() {
    return (Collection) sortedPartitions;
  }

  /**
   * Returns a collection of partitions for the given topic.
   *
   * @param topic the topic for which to return partitions
   * @return the partitions for the given topic
   */
  public Collection<Partition> getPartitions(String topic) {
    Map<PartitionId, LogPartition> partitions =  topics.get(topic);
    return partitions != null ? (Collection) partitions.values() : null;
  }

  @Override
  public List<PartitionId> getPartitionIds() {
    return sortedPartitionIds;
  }

  /**
   * Handles a log partition group metadata event.
   *
   * @param event the event to handle
   */
  private void onMetadataEvent(PartitionMetadataEvent<LogPartitionGroupMetadata> event) {
    MemberId localMemberId = managementService.getMembershipService().getLocalMemberId();
    for (LogTopicMetadata topic : event.metadata().getTopicsMap().values()) {
      if (!this.topics.containsKey(topic.getTopic())) {
        Map<PartitionId, LogPartition> partitions = new HashMap<>();
        for (int partitionId : topic.getPartitionIdsList()) {
          LogPartition partition = new LogPartition(PartitionId.newBuilder()
              .setGroup(config.getName())
              .setPartition(partitionId)
              .build(), config, topic, threadContextFactory);
          partitions.put(partition.id(), partition);
        }
        this.topics.put(topic.getTopic(), partitions);
        this.partitions.putAll(partitions);
      }

      if (managementService.getGroupMembershipService().getMembership(name).members().contains(localMemberId)) {
        if (!topic.getReadyList().contains(localMemberId.id())) {
          Futures.allOf(partitions.values().stream().map(p -> p.join(managementService)))
              .thenRun(() -> {
                managementService.getMetadataService().update(
                    name,
                    LogPartitionGroupMetadata::parseFrom,
                    metadata -> {
                      if (metadata == null) {
                        metadata = LogPartitionGroupMetadata.newBuilder().build();
                      }

                      LogTopicMetadata topicMetadata = metadata.getTopicsMap().get(topic.getTopic());
                      if (topicMetadata != null && !topicMetadata.getReadyList().contains(localMemberId.id())) {
                        return LogPartitionGroupMetadata.newBuilder(metadata)
                            .putTopics(topicMetadata.getTopic(), LogTopicMetadata.newBuilder(topic)
                                .addReady(localMemberId.id())
                                .build())
                            .build();
                      }
                      return metadata;
                    },
                    LogPartitionGroupMetadata::toByteArray);
              });
        } else if (topic.getReadyList().size() == managementService.getGroupMembershipService().getMembership(name).members().size()) {
          CompletableFuture<LogTopicMetadata> future = topicFutures.remove(topic.getTopic());
          if (future != null) {
            future.complete(topic);
          }
        }
      } else {
        partitions.values().forEach(p -> p.connect(managementService));
      }
    }
  }

  /**
   * Starts the partition group.
   *
   * @param managementService the partition management service
   * @return a future to be completed once the partition group has been started
   */
  private CompletableFuture<Void> start(PartitionManagementService managementService) {
    this.managementService = managementService;
    return managementService.getMetadataService().addListener(name, LogPartitionGroupMetadata::parseFrom, metadataListener)
    .thenCompose(v -> managementService.getMetadataService().get(
        name,
        LogPartitionGroupMetadata::parseFrom)
        .thenAccept(metadata -> {
          if (metadata != null) {
            for (LogTopicMetadata topic : metadata.getTopicsMap().values()) {
              Map<PartitionId, LogPartition> partitions = new HashMap<>();
              for (int partitionId : topic.getPartitionIdsList()) {
                LogPartition partition = new LogPartition(PartitionId.newBuilder()
                    .setGroup(config.getName())
                    .setPartition(++partitionId)
                    .build(), config, topic, threadContextFactory);
                partitions.put(partition.id(), partition);
              }
              this.topics.put(topic.getTopic(), partitions);
              this.partitions.putAll(partitions);
            }
          }
        }));
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> join(PartitionManagementService managementService) {
    return start(managementService).thenCompose(v -> {
      CompletableFuture[] futures = partitions.values().stream()
          .map(p -> p.join(managementService))
          .toArray(CompletableFuture[]::new);
      return CompletableFuture.allOf(futures);
    }).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  @Override
  public CompletableFuture<ManagedPartitionGroup> connect(PartitionManagementService managementService) {
    return start(managementService).thenCompose(v -> {
      CompletableFuture[] futures = partitions.values().stream()
          .map(p -> p.connect(managementService))
          .toArray(CompletableFuture[]::new);
      return CompletableFuture.allOf(futures);
    }).thenApply(v -> {
      LOGGER.info("Started");
      return this;
    });
  }

  /**
   * Creates a new topic in the partition group.
   *
   * @param topic the topic to create
   * @return a future to be completed once the topic has been created
   */
  public CompletableFuture<LogTopicMetadata> createTopic(LogTopicMetadata topic) {
    CompletableFuture<LogTopicMetadata> future = new CompletableFuture<>();
    topicFutures.put(topic.getTopic(), future);
    managementService.getMetadataService().update(
        name,
        LogPartitionGroupMetadata::parseFrom,
        logMetadata -> {
          if (logMetadata == null) {
            logMetadata = LogPartitionGroupMetadata.newBuilder().build();
          }

          LogTopicMetadata topicMetadata = logMetadata.getTopicsMap().get(topic.getTopic());
          if (topicMetadata == null) {
            int maxPartitionId = 0;
            for (LogTopicMetadata logTopicMetadata : logMetadata.getTopicsMap().values()) {
              for (int partitionId : logTopicMetadata.getPartitionIdsList()) {
                maxPartitionId = Math.max(maxPartitionId, partitionId);
              }
            }

            List<Integer> partitionIds = new ArrayList<>();
            for (int i = 0; i < topic.getPartitions(); i++) {
              partitionIds.add(++maxPartitionId);
            }
            topicMetadata = LogTopicMetadata.newBuilder(topic)
                .addAllPartitionIds(partitionIds)
                .build();
            return LogPartitionGroupMetadata.newBuilder(logMetadata)
                .putTopics(topic.getTopic(), topicMetadata)
                .build();
          }
          return logMetadata;
        },
        LogPartitionGroupMetadata::toByteArray)
        .whenComplete((metadata, error) -> {
          if (error == null) {
            LogTopicMetadata topicMetadata = metadata.getTopicsMap().get(topic.getTopic());
            if (topicMetadata.getReadyList().size() == managementService.getGroupMembershipService().getMembership(name).members().size()) {
              topicFutures.remove(topic.getTopic());
              future.complete(topicMetadata);
            }
          } else {
            topicFutures.remove(topic.getTopic());
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    List<CompletableFuture<Void>> futures = partitions.values().stream()
        .map(LogPartition::close)
        .collect(Collectors.toList());
    // Shutdown ThreadContextFactory on FJP common thread
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).whenCompleteAsync((r, e) -> {
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

  /**
   * Log partition group builder.
   */
  public static class Builder extends PartitionGroup.Builder<LogPartitionGroupConfig> {
    protected Builder(LogPartitionGroupConfig config) {
      super(config);
    }

    /**
     * Sets the member group strategy.
     *
     * @param memberGroupStrategy the member group strategy
     * @return the partition group builder
     */
    public Builder withMemberGroupStrategy(MemberGroupStrategy memberGroupStrategy) {
      config.setMemberGroupStrategy(memberGroupStrategy);
      return this;
    }

    /**
     * Sets the storage level.
     *
     * @param storageLevel the storage level
     * @return the Raft partition group builder
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      config.getStorageConfig().setLevel(storageLevel);
      return this;
    }

    /**
     * Sets the path to the data directory.
     *
     * @param dataDir the path to the replica's data directory
     * @return the replica builder
     */
    public Builder withDataDirectory(File dataDir) {
      config.getStorageConfig().setDirectory(new File("user.dir").toURI().relativize(dataDir.toURI()).getPath());
      return this;
    }

    /**
     * Sets the segment size.
     *
     * @param segmentSize the segment size
     * @return the partition group builder
     */
    public Builder withSegmentSize(MemorySize segmentSize) {
      config.getStorageConfig().setSegmentSize(segmentSize);
      return this;
    }

    /**
     * Sets the segment size.
     *
     * @param segmentSizeBytes the segment size in bytes
     * @return the partition group builder
     */
    public Builder withSegmentSize(long segmentSizeBytes) {
      return withSegmentSize(new MemorySize(segmentSizeBytes));
    }

    /**
     * Sets the maximum Raft log entry size.
     *
     * @param maxEntrySize the maximum Raft log entry size
     * @return the partition group builder
     */
    public Builder withMaxEntrySize(MemorySize maxEntrySize) {
      config.getStorageConfig().setMaxEntrySize(maxEntrySize);
      return this;
    }

    /**
     * Sets the maximum Raft log entry size.
     *
     * @param maxEntrySize the maximum Raft log entry size
     * @return the partition group builder
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      return withMaxEntrySize(new MemorySize(maxEntrySize));
    }

    /**
     * Enables flush on commit.
     *
     * @return the partition group builder
     */
    public Builder withFlushOnCommit() {
      return withFlushOnCommit(true);
    }

    /**
     * Sets whether to flush logs to disk on commit.
     *
     * @param flushOnCommit whether to flush logs to disk on commit
     * @return the partition group builder
     */
    public Builder withFlushOnCommit(boolean flushOnCommit) {
      config.getStorageConfig().setFlushOnCommit(flushOnCommit);
      return this;
    }

    /**
     * Sets the maximum size of the log.
     *
     * @param maxSize the maximum size of the log
     * @return the partition group builder
     */
    public Builder withMaxSize(long maxSize) {
      config.getCompactionConfig().setSize(MemorySize.from(maxSize));
      return this;
    }

    /**
     * Sets the maximum age of the log.
     *
     * @param maxAge the maximum age of the log
     * @return the partition group builder
     */
    public Builder withMaxAge(Duration maxAge) {
      config.getCompactionConfig().setAge(maxAge);
      return this;
    }

    @Override
    public LogPartitionGroup build() {
      return new LogPartitionGroup(config);
    }
  }
}
