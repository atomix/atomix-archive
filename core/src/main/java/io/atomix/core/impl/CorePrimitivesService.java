/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.AtomixRegistry;
import io.atomix.core.ManagedPrimitivesService;
import io.atomix.core.PrimitivesService;
import io.atomix.core.transaction.ManagedTransactionService;
import io.atomix.core.transaction.TransactionBuilder;
import io.atomix.core.transaction.TransactionConfig;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.transaction.impl.DefaultTransactionBuilder;
import io.atomix.primitive.ManagedPrimitiveRegistry;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.ConfigService;
import io.atomix.primitive.config.PrimitiveConfig;
import io.atomix.primitive.impl.DefaultPrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.impl.DefaultPartitionGroupTypeRegistry;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.impl.DefaultPrimitiveProtocolTypeRegistry;
import io.atomix.primitive.serialization.SerializationService;
import io.atomix.primitive.session.impl.DefaultSessionProtocolService;
import io.atomix.primitive.session.impl.ReplicatedSessionIdService;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default primitives service.
 */
public class CorePrimitivesService implements ManagedPrimitivesService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CorePrimitivesService.class);

  private final ClusterMembershipService membershipService;
  private final ClusterCommunicationService communicationService;
  private final ClusterEventService eventService;
  private final SerializationService serializationService;
  private final PartitionService partitionService;
  private final PrimitiveCache primitiveCache;
  private final ThreadContextFactory threadContextFactory;
  private final ManagedPrimitiveRegistry primitiveRegistry;
  private final ConfigService configService;
  private final AtomixRegistry registry;
  private PrimitiveManagementService managementService;
  private ManagedTransactionService transactionService;
  private final AtomicBoolean started = new AtomicBoolean();

  public CorePrimitivesService(
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      ClusterEventService eventService,
      SerializationService serializationService,
      PartitionService partitionService,
      PrimitiveCache primitiveCache,
      ThreadContextFactory threadContextFactory,
      AtomixRegistry registry,
      ConfigService configService) {
    this.membershipService = checkNotNull(membershipService);
    this.communicationService = checkNotNull(communicationService);
    this.eventService = checkNotNull(eventService);
    this.serializationService = checkNotNull(serializationService);
    this.partitionService = checkNotNull(partitionService);
    this.primitiveCache = checkNotNull(primitiveCache);
    this.threadContextFactory = checkNotNull(threadContextFactory);
    this.registry = checkNotNull(registry);
    this.primitiveRegistry = new CorePrimitiveRegistry(
        partitionService,
        membershipService,
        communicationService,
        new DefaultPrimitiveTypeRegistry(registry.getTypes(PrimitiveType.class)));
    this.configService = checkNotNull(configService);
  }

  /**
   * Returns the primitive management service.
   *
   * @return the primitive management service
   */
  public PrimitiveManagementService getManagementService() {
    return managementService;
  }

  /**
   * Returns the primitive transaction service.
   *
   * @return the primitive transaction service
   */
  public TransactionService transactionService() {
    return transactionService;
  }

  @Override
  public TransactionBuilder transactionBuilder(String name) {
    return new DefaultTransactionBuilder(name, new TransactionConfig(), managementService, transactionService);
  }

  @Override
  public PrimitiveType getPrimitiveType(String typeName) {
    return registry.getType(PrimitiveType.class, typeName);
  }

  @Override
  public <B extends PrimitiveBuilder<B, C, P>, C extends PrimitiveConfig<C>, P extends SyncPrimitive> B primitiveBuilder(
      String name, PrimitiveType<B, C, P> primitiveType) {
    return primitiveType.newBuilder(name, configService.getConfig(name, primitiveType), managementService);
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    return managementService.getPrimitiveRegistry().getPrimitives();
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return managementService.getPrimitiveRegistry().getPrimitives(primitiveType);
  }

  @Override
  public CompletableFuture<PrimitivesService> start() {
    return primitiveRegistry.start()
        .thenCompose(v -> transactionService.start())
        .thenRun(() -> {
          this.managementService = new CorePrimitiveManagementService(
              membershipService,
              communicationService,
              eventService,
              serializationService,
              partitionService,
              primitiveCache,
              primitiveRegistry,
              new DefaultPrimitiveTypeRegistry(registry.getTypes(PrimitiveType.class)),
              new DefaultPrimitiveProtocolTypeRegistry(registry.getTypes(PrimitiveProtocol.Type.class)),
              new DefaultPartitionGroupTypeRegistry(registry.getTypes(PartitionGroup.Type.class)),
              new ReplicatedSessionIdService(partitionService.getSystemPartitionGroup()),
              new DefaultSessionProtocolService(communicationService),
              threadContextFactory);
          this.transactionService = new CoreTransactionService(managementService);
        })
        .thenRun(() -> {
          LOGGER.info("Started");
          started.set(true);
        })
        .thenApply(v -> this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    return transactionService.stop().exceptionally(throwable -> {
      LOGGER.error("Failed stopping transaction service", throwable);
      return null;
    }).thenCompose(v -> primitiveRegistry.stop()).exceptionally(throwable -> {
      LOGGER.error("Failed stopping primitive registry", throwable);
      return null;
    }).whenComplete((r, e) -> {
      started.set(false);
      LOGGER.info("Stopped");
    });
  }
}
