package io.atomix.server.management.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.api.partition.PartitionId;
import io.atomix.server.ServerConfig;
import io.atomix.server.management.PartitionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;

/**
 * Partition service implementation.
 */
@Component(ServerConfig.class)
public class PartitionServiceImpl implements PartitionService, Managed<ServerConfig> {
  private PartitionId partitionId;

  @Override
  public PartitionId getPartitionId() {
    return partitionId;
  }

  @Override
  public CompletableFuture<Void> start(ServerConfig config) {
    partitionId = PartitionId.newBuilder()
        .setPartition(config.getPartitionId())
        .setGroup(config.getPartitionGroup())
        .build();
    return CompletableFuture.completedFuture(null);
  }
}
