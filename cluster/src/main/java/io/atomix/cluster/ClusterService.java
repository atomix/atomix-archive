package io.atomix.cluster;

/**
 * Cluster service.
 */
public interface ClusterService {

  /**
   * Returns the cluster membership service.
   * <p>
   * The membership service manages cluster membership information and failure detection.
   *
   * @return the cluster membership service
   */
  ClusterMembershipService getMembershipService();

}
