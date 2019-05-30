package io.atomix.client.partition;

import java.util.Collection;
import java.util.List;

/**
 * Partition group.
 */
public interface PartitionGroup {

  /**
   * Returns the partition group name.
   *
   * @return the partition group name
   */
  String name();

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
