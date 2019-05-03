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
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupMembershipEvent;
import io.atomix.primitive.partition.PartitionGroupMembershipEventListener;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.service.ServiceTypeRegistry;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
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
  private ClusterCommunicationService communicationService;

  @Dependency
  private ClusterStreamingService streamingService;

  @Dependency
  private ServiceTypeRegistry serviceTypeRegistry;

  @Dependency
  private PartitionGroupMembershipService groupMembershipService;

  @Dependency
  private PartitionManagementService partitionManagementService;

  @Dependency
  private SystemPartitionService systemPartitionService;

  private final Map<String, ManagedPartitionGroup> groups = Maps.newConcurrentMap();
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
    PartitionGroup systemGroup = systemPartitionService.getSystemPartitionGroup();
    if (systemGroup != null && systemGroup.name().equals(name)) {
      return systemGroup;
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<PartitionGroup> getPartitionGroups() {
    return (Collection) groups.values();
  }

  @SuppressWarnings("unchecked")
  private void handleMembershipChange(PartitionGroupMembershipEvent event) {
    if (partitionManagementService == null) {
      return;
    }

    if (!event.membership().system()) {
      synchronized (groups) {
        ManagedPartitionGroup group = groups.get(event.membership().group());
        if (group == null) {
          group = ((PartitionGroup.Type) event.membership().config().getType())
              .newPartitionGroup(event.membership().config());
          groups.put(event.membership().group(), group);
          if (event.membership().members().contains(clusterMembershipService.getLocalMember().id())) {
            group.join(partitionManagementService);
          } else {
            group.connect(partitionManagementService);
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
            group = groups.get(membership.group());
            if (group == null) {
              group = ((PartitionGroup.Type) membership.config().getType())
                  .newPartitionGroup(membership.config());
              groups.put(group.name(), group);
            }
          }
          if (membership.members().contains(clusterMembershipService.getLocalMember().id())) {
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
