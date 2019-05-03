/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.partition.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.event.AbstractListenable;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hash-based primary election service.
 */
@Component
public class SystemElectionManager
    extends AbstractListenable<PrimaryElectionEvent>
    implements PrimaryElectionService, Managed {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Dependency
  private ClusterMembershipService clusterMembershipService;
  @Dependency
  private PartitionGroupMembershipService groupMembershipService;
  @Dependency
  private ClusterCommunicationService messagingService;

  private final Map<PartitionId, HashBasedPrimaryElection> elections = Maps.newConcurrentMap();
  private final Consumer<PrimaryElectionEvent> primaryElectionListener = this::post;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
      Threads.namedThreads("primary-election-%d", log));

  @Override
  public PrimaryElection getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> {
      HashBasedPrimaryElection election = new HashBasedPrimaryElection(
          partitionId, clusterMembershipService, groupMembershipService, messagingService, executor);
      election.addListener(primaryElectionListener);
      return election;
    });
  }

  @Override
  public CompletableFuture<Void> start() {
    log.info("Started");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    elections.values().forEach(election -> election.close());
    executor.shutdownNow();
    return CompletableFuture.completedFuture(null);
  }
}
