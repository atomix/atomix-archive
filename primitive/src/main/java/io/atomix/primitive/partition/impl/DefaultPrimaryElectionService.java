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
package io.atomix.primitive.partition.impl;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.ManagedPrimaryElection;
import io.atomix.primitive.partition.ManagedPrimaryElectionService;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.serialization.SerializationService;
import io.atomix.primitive.session.ManagedSessionIdService;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.ReplicatedSessionIdService;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default primary election service.
 * <p>
 * This implementation uses a custom primitive service for primary election. The custom primitive service orders
 * candidates based on the existing distribution of primaries such that primaries are evenly spread across the cluster.
 */
public class DefaultPrimaryElectionService implements ManagedPrimaryElectionService {
  private static final String PRIMITIVE_NAME = "atomix-primary-elector";
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPrimaryElectionService.class);

  private final ClusterMembershipService membershipService;
  private final ManagedSessionIdService sessionIdService;
  private final ThreadContextFactory threadContextFactory;
  private final PrimaryElectorSession elector;
  private final Map<PartitionId, ManagedPrimaryElection> elections = Maps.newConcurrentMap();
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultPrimaryElectionService(ClusterMembershipService membershipService, PartitionGroup partitionGroup) {
    this.membershipService = membershipService;
    this.sessionIdService = new ReplicatedSessionIdService(partitionGroup);
    this.threadContextFactory = new BlockingAwareThreadPoolContextFactory("atomix-primary-election", 4, LOGGER);
    Partition partition = partitionGroup.getPartitions().iterator().next();
    this.elector = new PrimaryElectorSession(
        new PrimaryElectorProxy(new PrimitiveProxy.Context(
            PRIMITIVE_NAME,
            PrimaryElectorService.TYPE,
            partition,
            threadContextFactory)),
        Duration.ofSeconds(10),
        new PartialPrimitiveManagementService());
  }

  @Override
  @SuppressWarnings("unchecked")
  public PrimaryElection getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> new DefaultPrimaryElection(partitionId, elector));
  }

  @Override
  public void addListener(Consumer<PrimaryElectionEvent> listener) {
    try {
      elector.addListener(listener).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new PrimitiveException.Timeout();
    }
  }

  @Override
  public void removeListener(Consumer<PrimaryElectionEvent> listener) {
    try {
      elector.removeListener(listener).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new PrimitiveException.Timeout();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PrimaryElectionService> start() {
    return sessionIdService.start()
        .thenCompose(v -> elector.connect())
        .thenRun(() -> started.set(true))
        .thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      return elector.close()
          .thenCompose(v -> sessionIdService.stop());
    }
    return CompletableFuture.completedFuture(null);
  }

  private class PartialPrimitiveManagementService implements PrimitiveManagementService {
    @Override
    public ClusterMembershipService getMembershipService() {
      return membershipService;
    }

    @Override
    public ClusterCommunicationService getCommunicationService() {
      return null;
    }

    @Override
    public ClusterEventService getEventService() {
      return null;
    }

    @Override
    public SerializationService getSerializationService() {
      return null;
    }

    @Override
    public PartitionService getPartitionService() {
      return null;
    }

    @Override
    public PrimitiveCache getPrimitiveCache() {
      return null;
    }

    @Override
    public PrimitiveRegistry getPrimitiveRegistry() {
      return null;
    }

    @Override
    public PrimitiveTypeRegistry getPrimitiveTypeRegistry() {
      return null;
    }

    @Override
    public PrimitiveProtocolTypeRegistry getProtocolTypeRegistry() {
      return null;
    }

    @Override
    public PartitionGroupTypeRegistry getPartitionGroupTypeRegistry() {
      return null;
    }

    @Override
    public SessionIdService getSessionIdService() {
      return sessionIdService;
    }

    @Override
    public ThreadContextFactory getThreadFactory() {
      return threadContextFactory;
    }
  }
}
