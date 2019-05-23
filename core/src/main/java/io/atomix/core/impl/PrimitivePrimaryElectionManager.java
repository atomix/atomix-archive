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
package io.atomix.core.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.GrpcService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.election.LeaderElectionConfig;
import io.atomix.core.election.impl.DefaultLeaderElectionBuilder;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.PrimitiveSessionIdManager;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.serializer.Serializer;
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
  private static final String PRIMITIVE_NAME = "atomix-primary-elector";
  private static final Logger LOGGER = LoggerFactory.getLogger(PrimitivePrimaryElectionManager.class);

  @Dependency
  private ClusterMembershipService membershipService;
  @Dependency
  private PartitionGroupMembershipService groupMembershipService;
  @Dependency
  private GrpcService grpcService;
  @Dependency
  private PrimitiveSessionIdManager sessionIdService;
  @Dependency
  private SystemPartitionService systemService;

  private ThreadContextFactory threadContextFactory;
  private final Map<PartitionId, CompletableFuture<PrimaryElection>> elections = Maps.newConcurrentMap();

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PrimaryElection> getElectionFor(PartitionId partitionId) {
    return elections.computeIfAbsent(partitionId, id -> new DefaultLeaderElectionBuilder<GroupMember>(
        String.format("atomix-primary-election-%s-%d", id.getGroup(), id.getPartition()),
        new LeaderElectionConfig(),
        new PartialPrimitiveManagementService())
        .withProtocol(systemService.getSystemPartitionGroup().newProtocol())
        .withSerializer(new Serializer() {
          @Override
          public <T> byte[] encode(T object) {
            return ByteArrayEncoder.encode((GroupMember) object, GroupMember::toByteArray);
          }

          @Override
          public <T> T decode(byte[] bytes) {
            return (T) ByteArrayDecoder.decode(bytes, GroupMember::parseFrom);
          }
        }).buildAsync()
        .thenApply(election -> new PrimitivePrimaryElection(id, election.async())));
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
    public PartitionGroupMembershipService getGroupMembershipService() {
      return groupMembershipService;
    }

    @Override
    public GrpcService getGrpcService() {
      return grpcService;
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
