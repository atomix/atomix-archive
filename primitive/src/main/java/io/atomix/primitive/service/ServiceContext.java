package io.atomix.primitive.service;

import io.atomix.primitive.partition.PartitionId;

/**
 * Service context.
 */
public class ServiceContext {
  private final PartitionId partitionId;
  private final String name;

  public ServiceContext(PartitionId partitionId, String name) {
    this.partitionId = partitionId;
    this.name = name;
  }

  /**
   * Returns the service name.
   *
   * @return the service name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the partition ID.
   *
   * @return the partition ID
   */
  public PartitionId partitionId() {
    return partitionId;
  }
}
