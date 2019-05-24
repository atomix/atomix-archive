package io.atomix.server.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.grpc.ServiceRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.server.AtomixConfig;
import io.atomix.server.counter.CounterServiceImpl;
import io.atomix.server.election.LeaderElectionServiceImpl;
import io.atomix.server.lock.LockServiceImpl;
import io.atomix.server.log.LogServiceImpl;
import io.atomix.server.map.MapServiceImpl;
import io.atomix.server.set.SetServiceImpl;
import io.atomix.server.value.ValueServiceImpl;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Primitive manager.
 */
@Component(AtomixConfig.class)
public class PrimitiveManager implements Managed<AtomixConfig> {

  @Dependency
  private ServiceRegistry registry;

  @Dependency
  private PartitionService partitionService;

  @Override
  public CompletableFuture<Void> start(AtomixConfig config) {
    registry.register(new CounterServiceImpl(partitionService));
    registry.register(new LeaderElectionServiceImpl(partitionService));
    registry.register(new LockServiceImpl(partitionService));
    registry.register(new LogServiceImpl(partitionService));
    registry.register(new MapServiceImpl(partitionService));
    registry.register(new SetServiceImpl(partitionService));
    registry.register(new ValueServiceImpl(partitionService));
    return CompletableFuture.completedFuture(null);
  }
}
