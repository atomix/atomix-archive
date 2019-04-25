package io.atomix.primitive.partition.impl;

import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;

/**
 * Primary elector service type.
 */
public class PrimaryElectorServiceType implements ServiceType {
  private static final String NAME = "PRIMARY_ELECTOR";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
    return new PrimaryElectorService();
  }
}
