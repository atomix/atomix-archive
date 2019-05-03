package io.atomix.primitive.partition.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupMembership;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.service.ServiceTypeRegistry;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * System partition manager.
 */
@Component
public class SystemPartitionManager implements SystemPartitionService, PartitionManagementService, Managed {

  @Dependency
  private ClusterMembershipService clusterMembershipService;

  @Dependency
  private ClusterCommunicationService messagingService;

  @Dependency
  private ClusterStreamingService streamingService;

  @Dependency
  private ServiceTypeRegistry serviceTypeRegistry;

  @Dependency
  private SystemElectionManager systemElectionManager;

  @Dependency
  private PartitionGroupMembershipService groupMembershipService;

  private volatile ManagedPartitionGroup partitionGroup;

  @Override
  public ClusterMembershipService getMembershipService() {
    return clusterMembershipService;
  }

  @Override
  public ClusterCommunicationService getMessagingService() {
    return messagingService;
  }

  @Override
  public ClusterStreamingService getStreamingService() {
    return streamingService;
  }

  @Override
  public ServiceTypeRegistry getServiceTypes() {
    return serviceTypeRegistry;
  }

  @Override
  public PrimaryElectionService getElectionService() {
    return systemElectionManager;
  }

  @Override
  public PartitionGroup getSystemPartitionGroup() {
    return partitionGroup;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start() {
    PartitionGroupMembership systemGroupMembership = groupMembershipService.getSystemMembership();
    if (systemGroupMembership != null) {
      if (partitionGroup == null) {
        partitionGroup = ((PartitionGroup.Type) systemGroupMembership.config().getType())
            .newPartitionGroup(systemGroupMembership.config());
        if (systemGroupMembership.members().contains(clusterMembershipService.getLocalMember().id())) {
          return partitionGroup.join(this).thenApply(v -> null);
        } else {
          return partitionGroup.connect(this).thenApply(v -> null);
        }
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (partitionGroup != null) {
      return partitionGroup.close();
    }
    return CompletableFuture.completedFuture(null);
  }
}
