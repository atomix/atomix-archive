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
package io.atomix.server.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.impl.DefaultSessionClient;
import io.atomix.server.election.LeaderElectionProxy;
import io.atomix.server.election.LeaderElectionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default primary election service.
 * <p>
 * This implementation uses a custom primitive service for primary election. The custom primitive service orders
 * candidates based on the existing distribution of primaries such that primaries are evenly spread across the cluster.
 */
@Component
public class PrimitivePrimaryElectionManager implements PrimaryElectionService, Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(PrimitivePrimaryElectionManager.class);

  @Dependency
  private SystemPartitionService systemService;
  private ThreadContextFactory threadContextFactory;
  private final Map<PartitionId, CompletableFuture<PrimaryElection>> elections = Maps.newConcurrentMap();

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PrimaryElection> getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> {
      PartitionClient client = systemService.getSystemPartitionGroup().getPartitions().iterator().next().getClient();
      ServiceId serviceId = ServiceId.newBuilder()
          .setType(LeaderElectionService.TYPE.name())
          .setName(String.format("atomix-primary-election-%s-%d", id.getGroup(), id.getPartition()))
          .build();
      LeaderElectionProxy election = new LeaderElectionProxy(new DefaultSessionClient(serviceId, client));
      return new PrimitivePrimaryElection(election, threadContextFactory.createContext()).connect();
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start() {
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory("atomix-primary-election", 4, LOGGER);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return Futures.allOf(new ArrayList<>(elections.values()))
        .thenCompose(elections -> Futures.allOf(elections.stream()
            .map(election -> ((PrimitivePrimaryElection) election).close())
            .collect(Collectors.toList())))
        .thenRun(() -> threadContextFactory.close());
  }
}
