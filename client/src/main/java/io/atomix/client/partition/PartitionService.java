package io.atomix.client.partition;

import java.util.concurrent.CompletableFuture;

/**
 * Partition service.
 */
public interface PartitionService {

  /**
   * Returns a partition group by name.
   *
   * @param name the partition group name
   * @return the partition group
   */
  CompletableFuture<PartitionGroup> getPartitionGroup(String name);

}
