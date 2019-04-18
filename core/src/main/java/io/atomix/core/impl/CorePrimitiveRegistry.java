/*
 * Copyright 2018-present Open Networking Foundation
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapConfig;
import io.atomix.core.map.impl.DefaultAtomicMapBuilder;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.ManagedPrimitiveRegistry;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.serialization.SerializationService;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;

/**
 * Core primitive registry.
 */
public class CorePrimitiveRegistry implements ManagedPrimitiveRegistry {
  private static final Serializer SERIALIZER = Serializer.using(Namespaces.BASIC);

  private final PartitionService partitionService;
  private final ClusterMembershipService membershipService;
  private final ClusterCommunicationService communicationService;
  private final PrimitiveTypeRegistry primitiveTypeRegistry;
  private final AtomicBoolean started = new AtomicBoolean();
  private AsyncAtomicMap<String, String> primitives;

  public CorePrimitiveRegistry(
      PartitionService partitionService,
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      PrimitiveTypeRegistry primitiveTypeRegistry) {
    this.partitionService = partitionService;
    this.membershipService = membershipService;
    this.communicationService = communicationService;
    this.primitiveTypeRegistry = primitiveTypeRegistry;
  }

  @Override
  public CompletableFuture<PrimitiveInfo> createPrimitive(String name, PrimitiveType type) {
    PrimitiveInfo info = new PrimitiveInfo(name, type);
    CompletableFuture<PrimitiveInfo> future = new CompletableFuture<>();
    primitives.putIfAbsent(name, type.name()).whenComplete((result, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else if (result == null || result.value().equals(type.name())) {
        future.complete(info);
      } else {
        future.completeExceptionally(new PrimitiveException("A different primitive with the same name already exists"));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> deletePrimitive(String name) {
    return primitives.remove(name).thenApply(v -> null);
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives() {
    return primitives.sync().entrySet().stream()
        .map(entry -> new PrimitiveInfo(entry.getKey(), primitiveTypeRegistry.getPrimitiveType(entry.getValue().value())))
        .collect(Collectors.toList());
  }

  @Override
  public Collection<PrimitiveInfo> getPrimitives(PrimitiveType primitiveType) {
    return getPrimitives()
        .stream()
        .filter(primitive -> primitive.type().name().equals(primitiveType.name()))
        .collect(Collectors.toList());
  }

  @Override
  public PrimitiveInfo getPrimitive(String name) {
    try {
      return primitives.get(name)
          .thenApply(value -> value == null ? null : value.map(type -> new PrimitiveInfo(name, primitiveTypeRegistry.getPrimitiveType(type))).value())
          .get(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      throw new PrimitiveException(e.getCause());
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<PrimitiveRegistry> start() {
    return new DefaultAtomicMapBuilder<String, String>("primitives", new AtomicMapConfig(), new PartialPrimitiveManagementService())
        .withProtocol(partitionService.getSystemPartitionGroup().newProtocol())
        .buildAsync()
        .thenApply(map -> {
          this.primitives = map.async();
          started.set(true);
          return this;
        });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      return primitives.close().exceptionally(e -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private class PartialPrimitiveManagementService implements PrimitiveManagementService {
    @Override
    public ScheduledExecutorService getExecutorService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ClusterMembershipService getMembershipService() {
      return membershipService;
    }

    @Override
    public ClusterCommunicationService getCommunicationService() {
      return communicationService;
    }

    @Override
    public ClusterEventService getEventService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SerializationService getSerializationService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PartitionService getPartitionService() {
      return partitionService;
    }

    @Override
    public PrimitiveCache getPrimitiveCache() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveRegistry getPrimitiveRegistry() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveTypeRegistry getPrimitiveTypeRegistry() {
      return primitiveTypeRegistry;
    }

    @Override
    public PrimitiveProtocolTypeRegistry getProtocolTypeRegistry() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PartitionGroupTypeRegistry getPartitionGroupTypeRegistry() {
      throw new UnsupportedOperationException();
    }
  }
}
