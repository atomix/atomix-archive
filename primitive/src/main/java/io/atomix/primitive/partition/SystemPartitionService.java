package io.atomix.primitive.partition;

/**
 * System partition service.
 */
public interface SystemPartitionService {

  /**
   * Returns the system partition group.
   *
   * @return the system partition group
   */
  PartitionGroup getSystemPartitionGroup();

}
