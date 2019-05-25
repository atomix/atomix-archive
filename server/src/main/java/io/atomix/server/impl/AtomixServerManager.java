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

import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.VersionService;
import io.atomix.cluster.grpc.ServiceRegistry;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.server.AtomixConfig;
import io.atomix.server.AtomixService;
import io.atomix.server.counter.CounterServiceImpl;
import io.atomix.server.election.LeaderElectionServiceImpl;
import io.atomix.server.lock.LockServiceImpl;
import io.atomix.server.log.LogServiceImpl;
import io.atomix.server.map.MapServiceImpl;
import io.atomix.server.set.SetServiceImpl;
import io.atomix.server.value.ValueServiceImpl;
import io.atomix.utils.Version;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadService;

/**
 * Default primitives service.
 */
@Component(AtomixConfig.class)
public class AtomixServerManager implements AtomixService, Managed<AtomixConfig> {
  @Dependency
  private VersionService versionService;
  @Dependency
  private PartitionService partitions;
  @Dependency
  private PrimitiveProtocolTypeRegistry protocolTypes;
  @Dependency
  private PartitionGroupTypeRegistry partitionGroupTypes;
  @Dependency
  private ThreadService threadService;
  @Dependency
  private ClusterService clusterService;
  @Dependency
  private ServiceRegistry registry;

  @Override
  public Version getVersion() {
    return versionService.version();
  }

  @Override
  public ThreadContextFactory getThreadFactory() {
    return threadService.getFactory();
  }

  @Override
  public MessagingService getMessagingService() {
    return clusterService.getMessagingService();
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return clusterService.getMembershipService();
  }

  @Override
  public ClusterCommunicationService getCommunicationService() {
    return clusterService.getCommunicationService();
  }

  @Override
  public ClusterStreamingService getStreamingService() {
    return clusterService.getStreamingService();
  }

  @Override
  public ClusterEventService getEventService() {
    return clusterService.getEventService();
  }

  @Override
  public PartitionService getPartitionService() {
    return partitions;
  }

  @Override
  public PrimitiveProtocolTypeRegistry getProtocolTypes() {
    return protocolTypes;
  }

  @Override
  public PartitionGroupTypeRegistry getPartitionGroupTypes() {
    return partitionGroupTypes;
  }

  @Override
  public CompletableFuture<Void> start(AtomixConfig config) {
    registry.register(new CounterServiceImpl(partitions));
    registry.register(new LeaderElectionServiceImpl(partitions));
    registry.register(new LockServiceImpl(partitions));
    registry.register(new LogServiceImpl(partitions));
    registry.register(new MapServiceImpl(partitions));
    registry.register(new SetServiceImpl(partitions));
    registry.register(new ValueServiceImpl(partitions));
    return CompletableFuture.completedFuture(null);
  }
}
