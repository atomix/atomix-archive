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
package io.atomix.protocols.log.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.atomix.log.DistributedLogServer;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.MemberGroup;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.protocols.log.partition.LogPartition;
import io.atomix.protocols.log.partition.LogPartitionGroupConfig;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Primary-backup partition server.
 */
public class LogPartitionServer implements Managed<LogPartitionServer> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final LogPartition partition;
  private final PartitionManagementService managementService;
  private final LogPartitionGroupConfig config;
  private final ThreadContextFactory threadFactory;
  private DistributedLogServer server;
  private final AtomicBoolean started = new AtomicBoolean();

  public LogPartitionServer(
      LogPartition partition,
      PartitionManagementService managementService,
      LogPartitionGroupConfig config,
      ThreadContextFactory threadFactory) {
    this.partition = partition;
    this.managementService = managementService;
    this.config = config;
    this.threadFactory = threadFactory;
  }

  @Override
  public CompletableFuture<LogPartitionServer> start() {
    synchronized (this) {
      server = buildServer();
    }
    return server.start().thenApply(s -> {
      log.debug("Successfully started server for {}", partition.id());
      started.set(true);
      return this;
    });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  private DistributedLogServer buildServer() {
    MemberGroup memberGroup = config.getMemberGroupProvider()
        .getMemberGroups(managementService.getMembershipService().getMembers())
        .stream()
        .filter(group -> group.isMember(managementService.getMembershipService().getLocalMember()))
        .findAny()
        .orElse(null);
    return DistributedLogServer.builder()
        .withServerId(managementService.getMembershipService().getLocalMember().id().id())
        .withProtocol(new LogServerCommunicator(
            partition.name(),
            managementService.getMessagingService()))
        .withTermProvider(new LogPartitionTermProvider(
            managementService.getElectionService().getElectionFor(partition.id()),
            GroupMember.newBuilder()
                .setMemberId(managementService.getMembershipService().getLocalMember().id().id())
                .setMemberGroupId(memberGroup.id().id())
                .build(),
            config.getReplicationFactor()))
        .withStorageLevel(config.getStorageConfig().getLevel())
        .withDirectory(config.getStorageConfig().getDirectory(partition.name()))
        .withMaxSegmentSize((int) config.getStorageConfig().getSegmentSize().bytes())
        .withMaxEntrySize((int) config.getStorageConfig().getMaxEntrySize().bytes())
        .withFlushOnCommit(config.getStorageConfig().isFlushOnCommit())
        .withMaxLogSize(config.getCompactionConfig().getSize().bytes())
        .withMaxLogAge(config.getCompactionConfig().getAge())
        .withThreadContextFactory(threadFactory)
        .build();
  }

  @Override
  public CompletableFuture<Void> stop() {
    DistributedLogServer server = this.server;
    if (server != null) {
      return server.stop().exceptionally(throwable -> {
        log.error("Failed stopping server for {}", partition.id(), throwable);
        return null;
      }).thenRun(() -> started.set(false));
    }
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
