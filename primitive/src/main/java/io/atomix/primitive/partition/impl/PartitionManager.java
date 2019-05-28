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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Maps;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupMembershipEvent;
import io.atomix.primitive.partition.PartitionGroupMembershipEventListener;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.service.ServiceTypeRegistry;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default partition service.
 */
@Component
public class PartitionManager implements PartitionService, Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManager.class);

  @Dependency
  private ClusterMembershipService clusterMembershipService;
  @Dependency
  private ServiceTypeRegistry serviceTypeRegistry;
  @Dependency
  private PartitionGroupMembershipService groupMembershipService;
  @Dependency
  private PartitionGroupTypeRegistry groupTypeRegistry;
  @Dependency
  private PartitionManagementService partitionManagementService;
  @Dependency
  private SystemPartitionService systemPartitionService;

  private final Map<String, ManagedPartitionGroup> groups = Maps.newConcurrentMap();
  private final Map<String, ManagedPartitionGroup> configGroups = Maps.newConcurrentMap();
  private final Map<String, ManagedPartitionGroup> protocolGroups = Maps.newConcurrentMap();
  private final PartitionGroupMembershipEventListener groupMembershipEventListener = this::handleMembershipChange;

  @Override
  @SuppressWarnings("unchecked")
  public PartitionGroup getSystemPartitionGroup() {
    return systemPartitionService.getSystemPartitionGroup();
  }

  @Override
  @SuppressWarnings("unchecked")
  public PartitionGroup getPartitionGroup(String name) {
    PartitionGroup group = groups.get(name);
    if (group != null) {
      return group;
    }
    group = configGroups.get(name);
    if (group != null) {
      return group;
    }
    group = protocolGroups.get(name);
    if (group != null) {
      return group;
    }

    PartitionGroup systemGroup = systemPartitionService.getSystemPartitionGroup();
    if (systemGroup != null
        && (systemGroup.name().equals(name)
        || systemGroup.type().getConfigDescriptor().getFullName().equals(name)
        || systemGroup.type().getProtocolDescriptor().getFullName().equals(name))) {
      return systemGroup;
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<PartitionGroup> getPartitionGroups() {
    return (Collection) groups.values();
  }

  private String getType(PartitionGroupConfig config) {
    if (config.hasRaft()) {
      return "raft";
    }
    if (config.hasPrimaryBackup()) {
      return "primary-backup";
    }
    if (config.hasLog()) {
      return "log";
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private void handleMembershipChange(PartitionGroupMembershipEvent event) {
    if (partitionManagementService == null) {
      return;
    }

    if (!event.membership().getSystem()) {
      synchronized (groups) {
        ManagedPartitionGroup group = groups.get(event.membership().getConfig().getName());
        if (group == null) {
          PartitionGroup.Type type = groupTypeRegistry.getGroupType(getType(event.membership().getConfig()));
          if (type != null) {
            group = type.newPartitionGroup(event.membership().getConfig());
            groups.put(group.name(), group);
            protocolGroups.put(group.type().getProtocolDescriptor().getFullName(), group);
            if (event.membership().getMembersList().contains(MemberId.from(clusterMembershipService.getLocalMember()).toString())) {
              group.join(partitionManagementService);
            } else {
              group.connect(partitionManagementService);
            }
          } else {
            LOGGER.error("Unknown partition group type " + getType(event.membership().getConfig()));
          }
        }
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start() {
    groupMembershipService.addListener(groupMembershipEventListener);
    CompletableFuture[] futures = groupMembershipService.getMemberships().stream()
        .map(membership -> {
          ManagedPartitionGroup group;
          synchronized (groups) {
            group = groups.get(membership.getConfig().getName());
            if (group == null) {
              PartitionGroup.Type type = groupTypeRegistry.getGroupType(getType(membership.getConfig()));
              if (type != null) {
                group = type.newPartitionGroup(membership.getConfig());
                groups.put(group.name(), group);
                protocolGroups.put(group.type().getProtocolDescriptor().getFullName(), group);
              } else {
                return Futures.exceptionalFuture(new IllegalStateException("Unknown partition group type " + getType(membership.getConfig())));
              }
            }
          }
          if (membership.getMembersList().contains(MemberId.from(clusterMembershipService.getLocalMember()).toString())) {
            return group.join(partitionManagementService);
          } else {
            return group.connect(partitionManagementService);
          }
        })
        .toArray(CompletableFuture[]::new);
    return CompletableFuture.allOf(futures).thenRun(() -> {
      LOGGER.info("Started");
    });
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> stop() {
    groupMembershipService.removeListener(groupMembershipEventListener);
    return CompletableFuture.allOf(groups.values()
        .stream()
        .map(ManagedPartitionGroup::close)
        .toArray(CompletableFuture[]::new))
        .thenRun(() -> {
          LOGGER.info("Stopped");
        });
  }
}
