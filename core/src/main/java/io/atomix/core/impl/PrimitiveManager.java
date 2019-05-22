package io.atomix.core.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.GrpcService;
import io.atomix.core.AtomixConfig;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Primitive manager.
 */
@Component(AtomixConfig.class)
public class PrimitiveManager implements Managed<AtomixConfig> {

  @Dependency
  private GrpcService grpc;

  @Dependency
  private PartitionService partitionService;

  @Override
  public CompletableFuture<Void> start(AtomixConfig config) {
    grpc.register(new CounterServiceImpl(partitionService));
    grpc.register(new LeaderElectionServiceImpl(partitionService));
    grpc.register(new LockServiceImpl(partitionService));
    grpc.register(new LogServiceImpl(partitionService));
    grpc.register(new MapServiceImpl(partitionService));
    grpc.register(new SetServiceImpl(partitionService));
    grpc.register(new ValueServiceImpl(partitionService));
    return CompletableFuture.completedFuture(null);
  }
}
