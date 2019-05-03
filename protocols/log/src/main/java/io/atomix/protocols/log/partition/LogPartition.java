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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.service.impl.ServiceManagerStateMachine;
import io.atomix.protocols.log.partition.impl.LogPartitionClient;
import io.atomix.protocols.log.partition.impl.LogPartitionServer;
import io.atomix.protocols.log.partition.impl.LogPartitionSession;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContextFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Log partition.
 */
public class LogPartition implements Partition {
  private final PartitionId partitionId;
  private final LogPartitionGroupConfig config;
  private final ThreadContextFactory threadContextFactory;
  private PartitionManagementService managementService;
  private PrimaryElection election;
  private LogPartitionServer server;
  private volatile LogPartitionClient client;
  private LogPartitionSession session;

  public LogPartition(
      PartitionId partitionId,
      LogPartitionGroupConfig config,
      ThreadContextFactory threadContextFactory) {
    this.partitionId = partitionId;
    this.config = config;
    this.threadContextFactory = threadContextFactory;
  }

  @Override
  public PartitionId id() {
    return partitionId;
  }

  @Override
  public long term() {
    return Futures.get(election.getTerm()).getTerm();
  }

  @Override
  public Collection<MemberId> members() {
    return Futures.get(election.getTerm())
        .getCandidatesList()
        .stream()
        .map(GroupMember::getMemberId)
        .map(MemberId::from)
        .collect(Collectors.toList());
  }

  @Override
  public MemberId primary() {
    return MemberId.from(Futures.get(election.getTerm())
        .getPrimary()
        .getMemberId());
  }

  @Override
  public Collection<MemberId> backups() {
    return Futures.get(election.getTerm())
        .getCandidatesList()
        .stream()
        .map(GroupMember::getMemberId)
        .map(MemberId::from)
        .collect(Collectors.toList());
  }

  /**
   * Returns the partition name.
   *
   * @return the partition name
   */
  public String name() {
    return String.format("%s-partition-%d", partitionId.getGroup(), partitionId.getPartition());
  }

  @Override
  public PartitionClient getClient() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = new LogPartitionClient(
              session,
              new ServiceManagerStateMachine(id(), managementService),
              threadContextFactory.createContext(),
              threadContextFactory.createContext());
        }
      }
    }
    return client;
  }

  /**
   * Returns the log partition client.
   *
   * @return the log partition client
   */
  public LogSession getSession() {
    return session;
  }

  /**
   * Joins the log partition.
   */
  CompletableFuture<Partition> join(PartitionManagementService managementService) {
    this.managementService = managementService;
    election = managementService.getElectionService().getElectionFor(partitionId);
    server = new LogPartitionServer(
        this,
        managementService,
        config,
        threadContextFactory);
    return server.start()
        .thenCompose(v -> {
          session = new LogPartitionSession(this, managementService, config, threadContextFactory);
          return session.start();
        }).thenApply(v -> this);
  }

  /**
   * Connects to the log partition.
   */
  CompletableFuture<Partition> connect(PartitionManagementService managementService) {
    election = managementService.getElectionService().getElectionFor(partitionId);
    session = new LogPartitionSession(this, managementService, config, threadContextFactory);
    return session.start().thenApply(v -> this);
  }

  /**
   * Closes the log partition.
   */
  public CompletableFuture<Void> close() {
    if (session == null) {
      return CompletableFuture.completedFuture(null);
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    session.stop().whenComplete((clientResult, clientError) -> {
      if (server != null) {
        server.stop().whenComplete((serverResult, serverError) -> {
          future.complete(null);
        });
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", partitionId)
        .toString();
  }
}
