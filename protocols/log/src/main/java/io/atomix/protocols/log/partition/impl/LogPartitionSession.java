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

import io.atomix.log.DistributedLogClient;
import io.atomix.primitive.log.LogConsumer;
import io.atomix.primitive.log.LogProducer;
import io.atomix.primitive.log.LogSession;
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
 * Primary-backup partition client.
 */
public class LogPartitionSession implements LogSession, Managed<LogPartitionSession> {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final LogPartition partition;
  private final PartitionManagementService managementService;
  private final LogPartitionGroupConfig config;
  private final ThreadContextFactory threadFactory;
  private volatile DistributedLogClient client;

  public LogPartitionSession(
      LogPartition partition,
      PartitionManagementService managementService,
      LogPartitionGroupConfig config,
      ThreadContextFactory threadFactory) {
    this.partition = partition;
    this.config = config;
    this.managementService = managementService;
    this.threadFactory = threadFactory;
  }

  @Override
  public LogProducer producer() {
    return new LogPartitionProducer(client.producer());
  }

  @Override
  public LogConsumer consumer() {
    return new LogPartitionConsumer(client.consumer());
  }

  @Override
  public CompletableFuture<LogPartitionSession> start() {
    synchronized (LogPartitionSession.this) {
      client = newClient();
      log.debug("Successfully started client for {}", partition.id());
    }
    return client.connect().thenApply(v -> this);
  }

  private DistributedLogClient newClient() {
    MemberGroup memberGroup = config.getMemberGroupProvider()
        .getMemberGroups(managementService.getMembershipService().getMembers())
        .stream()
        .filter(group -> group.isMember(managementService.getMembershipService().getLocalMember()))
        .findAny()
        .orElse(null);
    return DistributedLogClient.builder()
        .withClientId(managementService.getMembershipService().getLocalMember().id().id())
        .withProtocol(new LogClientCommunicator(
            partition.name(),
            managementService.getMessagingService()))
        .withTermProvider(new LogPartitionTermProvider(
            managementService.getElectionService().getElectionFor(partition.id()),
            GroupMember.newBuilder()
                .setMemberId(managementService.getMembershipService().getLocalMember().id().id())
                .setMemberGroupId(memberGroup.id().id())
                .build(),
            config.getReplicationFactor()))
        .withThreadContextFactory(threadFactory)
        .build();
  }

  @Override
  public boolean isRunning() {
    return client != null;
  }

  @Override
  public CompletableFuture<Void> stop() {
    return client != null ? client.close() : CompletableFuture.completedFuture(null);
  }
}
