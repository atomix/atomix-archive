package io.atomix.primitive.partition.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.partition.ManagedPartitionGroup;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupConfig;
import io.atomix.primitive.partition.PartitionGroupMembership;
import io.atomix.primitive.partition.PartitionGroupMembershipService;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.partition.PartitionMetadataService;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.partition.SystemElectionService;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.service.ServiceTypeRegistry;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Futures;

/**
 * System partition manager.
 */
@Component
public class SystemPartitionManager implements SystemPartitionService, PartitionManagementService, Managed {

  @Dependency
  private ClusterMembershipService clusterMembershipService;
  @Dependency
  private ServiceTypeRegistry serviceTypeRegistry;
  @Dependency
  private SystemElectionService systemElectionService;
  @Dependency
  private PartitionGroupMembershipService groupMembershipService;
  @Dependency
  private PartitionGroupTypeRegistry groupTypeRegistry;

  private volatile ManagedPartitionGroup partitionGroup;

  @Override
  public ClusterMembershipService getMembershipService() {
    return clusterMembershipService;
  }

  @Override
  public PartitionMetadataService getMetadataService() {
    return null;
  }

  @Override
  public ServiceTypeRegistry getServiceTypes() {
    return serviceTypeRegistry;
  }

  @Override
  public PrimaryElectionService getElectionService() {
    return systemElectionService;
  }

  @Override
  public PartitionGroup getSystemPartitionGroup() {
    return partitionGroup;
  }

  @Override
  public PartitionGroupMembershipService getGroupMembershipService() {
    return groupMembershipService;
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

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start() {
    PartitionGroupMembership systemGroupMembership = groupMembershipService.getSystemMembership();
    if (systemGroupMembership != null) {
      if (partitionGroup == null) {
        PartitionGroup.Type type = groupTypeRegistry.getGroupType(getType(systemGroupMembership.getConfig()));
        if (type != null) {
          partitionGroup = type.newPartitionGroup(systemGroupMembership.getConfig());
          if (systemGroupMembership.getMembersList().contains(MemberId.from(clusterMembershipService.getLocalMember()).toString())) {
            return partitionGroup.join(this).thenApply(v -> null);
          } else {
            return partitionGroup.connect(this).thenApply(v -> null);
          }
        } else {
          return Futures.exceptionalFuture(new IllegalStateException("Unknown partition group type " + getType(systemGroupMembership.getConfig())));
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
