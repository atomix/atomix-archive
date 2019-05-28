package io.atomix.cluster.impl;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ClusterService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Cluster manager.
 */
@Component(ClusterConfig.class)
public class ClusterManager implements ClusterService, Managed<ClusterConfig> {
  @Dependency
  private ClusterMembershipService membershipService;

  @Override
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }
}
