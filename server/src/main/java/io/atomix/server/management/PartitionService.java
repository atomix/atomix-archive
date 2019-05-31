package io.atomix.server.management;

import io.atomix.api.partition.PartitionId;

/**
 * Partition ID service.
 */
public interface PartitionService {

  /**
   * Returns the partition ID.
   *
   * @return the partition ID
   */
  PartitionId getPartitionId();

}
