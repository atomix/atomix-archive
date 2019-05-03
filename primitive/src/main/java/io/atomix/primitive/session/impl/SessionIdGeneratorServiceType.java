package io.atomix.primitive.session.impl;

import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.ServiceType;
import io.atomix.utils.component.Component;

/**
 * Session ID generator service type.
 */
@Component
public class SessionIdGeneratorServiceType implements ServiceType {
  private static final String NAME = "SESSION_ID_GENERATOR";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public PrimitiveService newService(PartitionId partitionId, PartitionManagementService managementService) {
    return new SessionIdGeneratorService();
  }
}
