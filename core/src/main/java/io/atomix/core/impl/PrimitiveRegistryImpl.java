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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.impl.MapProxy;
import io.atomix.core.map.impl.MapService;
import io.atomix.core.map.impl.PartitionedAsyncAtomicMap;
import io.atomix.core.map.impl.RawAsyncAtomicMap;
import io.atomix.core.map.impl.TranscodingAsyncAtomicMap;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveInfo;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.PrimitiveSessionIdManager;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadService;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Core primitive registry.
 */
@Component
public class PrimitiveRegistryImpl implements PrimitiveRegistry, Managed {

  @Dependency
  private PartitionService partitionService;

  @Dependency
  private ClusterMembershipService membershipService;

  @Dependency
  private ClusterCommunicationService communicationService;

  @Dependency
  private PrimitiveTypeRegistry primitiveTypeRegistry;

  @Dependency
  private PrimitiveSessionIdManager sessionIdManager;

  @Dependency
  private ThreadService threadService;

  private AsyncAtomicMap<String, String> primitives;

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
  public CompletableFuture<Void> start() {
    Map<PartitionId, RawAsyncAtomicMap> partitions = partitionService.getSystemPartitionGroup().getPartitions().stream()
        .map(partition -> {
          MapProxy proxy = new MapProxy(new PrimitiveProxy.Context(
              "primitives", MapService.TYPE, partition, threadService.getFactory()));
          return Pair.of(partition.id(), new RawAsyncAtomicMap(proxy, Duration.ofSeconds(30), new PartialPrimitiveManagementService()));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    return new PartitionedAsyncAtomicMap(
        "primitives",
        AtomicMapType.instance(),
        (Map) partitions,
        Partitioner.MURMUR3)
        .connect()
        .thenApply(map -> new TranscodingAsyncAtomicMap<String, String, String, byte[]>(
            map,
            key -> key,
            key -> key,
            value -> value.getBytes(StandardCharsets.UTF_8),
            bytes -> new String(bytes, StandardCharsets.UTF_8)))
        .thenAccept(map -> {
          this.primitives = map;
        });
  }

  @Override
  public CompletableFuture<Void> stop() {
    return primitives.close();
  }

  private class PartialPrimitiveManagementService implements PrimitiveManagementService {
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

    @Override
    public SessionIdService getSessionIdService() {
      return sessionIdManager;
    }

    @Override
    public ThreadContextFactory getThreadFactory() {
      return threadService.getFactory();
    }
  }
}
