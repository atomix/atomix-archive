package io.atomix.client.partition;

import java.util.concurrent.CompletableFuture;

import io.atomix.api.partition.PartitionGroupId;

/**
 * Partition service.
 */
public interface PartitionService {

  /**
   * Returns a partition group by name.
   *
   * @param id the partition group ID
   * @return the partition group
   */
  CompletableFuture<PartitionGroup> getPartitionGroup(PartitionGroupId id);

}
