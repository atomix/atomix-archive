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

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberEvent;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.grpc.ServiceFactory;
import io.atomix.cluster.grpc.ServiceProvider;
import io.atomix.cluster.grpc.ServiceRegistry;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupInfo;
import io.atomix.primitive.partition.PartitionGroupMembership;
import io.atomix.primitive.partition.PartitionGroupMembershipEvent;
import io.atomix.primitive.partition.PartitionGroupMembershipEventListener;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionGroupMembershipServiceGrpc;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupsConfig;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.config.ConfigurationException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.atomix.primitive.partition.PartitionGroupMembershipEvent.Type.MEMBERS_CHANGED;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Default partition group membership service.
 */
@Component(PartitionGroupsConfig.class)
public class PartitionGroupMembershipManager
    extends PartitionGroupMembershipServiceGrpc.PartitionGroupMembershipServiceImplBase
    implements PartitionGroupMembershipService, Managed<PartitionGroupsConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionGroupMembershipManager.class);
  private static final String BOOTSTRAP_SUBJECT = "partition-group-bootstrap";
  private static final int[] FIBONACCI_NUMBERS = new int[]{1, 1, 2, 3, 5};
  private static final int MAX_PARTITION_GROUP_ATTEMPTS = 5;

  @Dependency
  private ClusterMembershipService membershipService;
  @Dependency
  private PartitionGroupTypeRegistry groupTypeRegistry;
  @Dependency
  private ServiceProvider serviceProvider;
  @Dependency
  private ServiceRegistry serviceRegistry;

  private volatile PartitionGroupMembership systemGroup;
  private final Map<String, PartitionGroupMembership> groups = Maps.newConcurrentMap();
  private final Consumer<MemberEvent> membershipEventListener = this::handleMembershipChange;
  private final Set<PartitionGroupMembershipEventListener> eventListeners = new CopyOnWriteArraySet<>();
  private final AtomicBoolean started = new AtomicBoolean();
  private ServiceFactory<PartitionGroupMembershipServiceGrpc.PartitionGroupMembershipServiceStub> serviceFactory;
  private volatile ThreadContext threadContext;

  @Override
  public void addListener(PartitionGroupMembershipEventListener listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeListener(PartitionGroupMembershipEventListener listener) {
    eventListeners.remove(listener);
  }

  private void post(PartitionGroupMembershipEvent event) {
    eventListeners.forEach(l -> l.event(event));
  }

  @Override
  public PartitionGroupMembership getSystemMembership() {
    return systemGroup;
  }

  @Override
  public PartitionGroupMembership getMembership(String group) {
    PartitionGroupMembership membership = groups.get(group);
    if (membership != null) {
      return membership;
    }
    return systemGroup.getConfig().getName().equals(group) ? systemGroup : null;
  }

  @Override
  public Collection<PartitionGroupMembership> getMemberships() {
    return groups.values();
  }

  /**
   * Handles a cluster membership change.
   */
  private void handleMembershipChange(MemberEvent event) {
    if (event.getType() == MemberEvent.Type.ADDED) {
      bootstrap(event.getMember());
    } else if (event.getType() == MemberEvent.Type.REMOVED) {
      threadContext.execute(() -> {
        PartitionGroupMembership systemGroup = this.systemGroup;
        if (systemGroup != null && systemGroup.getMembersList().contains(MemberId.from(event.getMember()).toString())) {
          Set<String> newMembers = Sets.newHashSet(systemGroup.getMembersList());
          newMembers.remove(MemberId.from(event.getMember()).toString());
          PartitionGroupMembership newMembership = PartitionGroupMembership.newBuilder(systemGroup)
              .addAllMembers(newMembers)
              .build();
          this.systemGroup = newMembership;
          post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newMembership));
        }

        groups.values().forEach(group -> {
          if (group.getMembersList().contains(MemberId.from(event.getMember()).toString())) {
            Set<String> newMembers = Sets.newHashSet(group.getMembersList());
            newMembers.remove(MemberId.from(event.getMember()).toString());
            PartitionGroupMembership newMembership = PartitionGroupMembership.newBuilder(systemGroup)
                .addAllMembers(newMembers)
                .build();
            groups.put(group.getConfig().getName(), newMembership);
            post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newMembership));
          }
        });
      });
    }
  }

  /**
   * Bootstraps the service.
   */
  private CompletableFuture<Void> bootstrap() {
    return bootstrap(0, new CompletableFuture<>());
  }

  /**
   * Recursively bootstraps the service, retrying if necessary until a system partition group is found.
   */
  private CompletableFuture<Void> bootstrap(int attempt, CompletableFuture<Void> future) {
    Futures.allOf(membershipService.getMembers().stream()
        .filter(node -> !MemberId.from(node).equals(MemberId.from(membershipService.getLocalMember())))
        .map(node -> bootstrap(node))
        .collect(Collectors.toList()))
        .whenComplete((result, error) -> {
          if (error == null) {
            if (systemGroup == null) {
              LOGGER.warn("Failed to locate management group via bootstrap nodes. Please ensure partition "
                  + "groups are configured either locally or remotely and the node is able to reach partition group members.");
              threadContext.schedule(Duration.ofSeconds(FIBONACCI_NUMBERS[Math.min(attempt, 4)]), () -> bootstrap(attempt + 1, future));
            } else if (groups.isEmpty() && attempt < MAX_PARTITION_GROUP_ATTEMPTS) {
              LOGGER.warn("Failed to locate partition group(s) via bootstrap nodes. Please ensure partition "
                  + "groups are configured either locally or remotely and the node is able to reach partition group members.");
              threadContext.schedule(Duration.ofSeconds(FIBONACCI_NUMBERS[Math.min(attempt, 4)]), () -> bootstrap(attempt + 1, future));
            } else {
              future.complete(null);
            }
          } else {
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  /**
   * Bootstraps the service from the given node.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> bootstrap(Member member) {
    return bootstrap(member, new CompletableFuture<>());
  }

  /**
   * Bootstraps the service from the given node.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> bootstrap(Member member, CompletableFuture<Void> future) {
    LOGGER.debug("{} - Bootstrapping from member {}", MemberId.from(membershipService.getLocalMember()), member);
    PartitionGroupInfo.Builder builder = PartitionGroupInfo.newBuilder()
        .setMemberId(MemberId.from(membershipService.getLocalMember()).toString());
    if (systemGroup != null) {
      builder.setSystemGroup(systemGroup);
    }
    builder.addAllGroups(groups.values());
    serviceFactory.getService(MemberId.from(member))
        .bootstrap(builder.build(), new StreamObserver<PartitionGroupInfo>() {
          @Override
          public void onNext(PartitionGroupInfo info) {
            try {
              updatePartitionGroups(info);
              future.complete(null);
            } catch (Exception e) {
              future.completeExceptionally(e);
            }
          }

          @Override
          public void onError(Throwable t) {
            threadContext.schedule(Duration.ofSeconds(1), () -> bootstrap(member, future));
          }

          @Override
          public void onCompleted() {
            future.complete(null);
          }
        });
    return future;
  }

  private void updatePartitionGroups(PartitionGroupInfo info) {
    if (systemGroup == null && info.hasSystemGroup()) {
      systemGroup = info.getSystemGroup();
      post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, systemGroup));
      LOGGER.info("{} - Bootstrapped management group {} from {}", MemberId.from(membershipService.getLocalMember()), systemGroup, info.getMemberId());
    } else if (systemGroup != null && info.hasSystemGroup()) {
      if (!systemGroup.getConfig().getName().equals(info.getSystemGroup().getConfig().getName())) {
        throw new ConfigurationException("Duplicate system group detected");
      } else {
        Set<String> newMembers = Stream.concat(systemGroup.getMembersList().stream(), info.getSystemGroup().getMembersList().stream())
            .filter(memberId -> membershipService.getMember(memberId) != null)
            .collect(Collectors.toSet());
        if (!Sets.difference(newMembers, new HashSet<>(systemGroup.getMembersList())).isEmpty()) {
          systemGroup = PartitionGroupMembership.newBuilder(systemGroup)
              .addAllMembers(newMembers)
              .build();
          post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, systemGroup));
          LOGGER.debug("{} - Updated management group {} from {}", MemberId.from(membershipService.getLocalMember()), systemGroup, info.getMemberId());
        }
      }
    }

    for (PartitionGroupMembership newMembership : info.getGroupsList()) {
      PartitionGroupMembership oldMembership = groups.get(newMembership.getConfig().getName());
      if (oldMembership == null) {
        groups.put(newMembership.getConfig().getName(), newMembership);
        post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newMembership));
        LOGGER.info("{} - Bootstrapped partition group {} from {}", MemberId.from(membershipService.getLocalMember()), newMembership, info.getMemberId());
      } else if (!oldMembership.getConfig().getName().equals(newMembership.getConfig().getName())) {
        throw new ConfigurationException("Duplicate partition group " + newMembership.getConfig().getName() + " detected");
      } else {
        Set<String> newMembers = Stream.concat(oldMembership.getMembersList().stream(), newMembership.getMembersList().stream())
            .filter(memberId -> membershipService.getMember(memberId) != null)
            .collect(Collectors.toSet());
        if (!Sets.difference(newMembers, new HashSet<>(oldMembership.getMembersList())).isEmpty()) {
          PartitionGroupMembership newGroup = PartitionGroupMembership.newBuilder(oldMembership)
              .addAllMembers(newMembers)
              .build();
          groups.put(oldMembership.getConfig().getName(), newGroup);
          post(new PartitionGroupMembershipEvent(MEMBERS_CHANGED, newGroup));
          LOGGER.debug("{} - Updated partition group {} from {}", MemberId.from(membershipService.getLocalMember()), newGroup, info.getMemberId());
        }
      }
    }
  }

  @Override
  public void bootstrap(PartitionGroupInfo info, StreamObserver<PartitionGroupInfo> responseObserver) {
    try {
      updatePartitionGroups(info);
      responseObserver.onNext(PartitionGroupInfo.newBuilder()
          .setMemberId(MemberId.from(membershipService.getLocalMember()).toString())
          .setSystemGroup(systemGroup)
          .addAllGroups(groups.values())
          .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      // Log the exception
      LOGGER.warn("{}", e.getMessage());
      responseObserver.onError(e);
    }
  }

  @Override
  public CompletableFuture<Void> start(PartitionGroupsConfig config) {
    this.systemGroup = config.getSystem() != null
        ? PartitionGroupMembership.newBuilder()
        .setConfig(config.getSystem())
        .addMembers(MemberId.from(membershipService.getLocalMember()).toString())
        .setSystem(true)
        .build()
        : null;
    config.getGroupsList().forEach(group -> {
      this.groups.put(group.getName(), PartitionGroupMembership.newBuilder()
          .setConfig(group)
          .addMembers(MemberId.from(membershipService.getLocalMember()).toString())
          .setSystem(false)
          .build());
    });

    List<PartitionGroup.Type> groupTypes = Lists.newArrayList(groupTypeRegistry.getGroupTypes());
    groupTypes.sort(Comparator.comparing(PartitionGroup.Type::name));

    threadContext = new SingleThreadContext(namedThreads("atomix-partition-group-membership-service-%d", LOGGER));
    membershipService.addListener(membershipEventListener);
    return bootstrap().thenRun(() -> {
      LOGGER.info("Started");
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> stop() {
    membershipService.removeListener(membershipEventListener);
    ThreadContext threadContext = this.threadContext;
    if (threadContext != null) {
      threadContext.close();
    }
    LOGGER.info("Stopped");
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
