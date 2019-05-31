package io.atomix.client.partition;

import java.util.Collection;
import java.util.List;

import io.atomix.api.partition.PartitionGroupId;

/**
 * Partition group.
 */
public interface PartitionGroup {

  /**
   * Returns the partition group ID.
   *
   * @return the partition group ID
   */
  PartitionGroupId id();

  /**
   * Returns a partition by ID.
   *
   * @param partitionId the partition ID
   * @return the partition
   */
  Partition getPartition(int partitionId);

  /**
   * Returns the partitions in the group.
   *
   * @return the partitions in the group
   */
  Collection<Partition> getPartitions();

  /**
   * Returns a list of partition IDs in the partition group.
   *
   * @return a list of partition IDs
   */
  List<Integer> getPartitionIds();

}
