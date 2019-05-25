package io.atomix.server.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.ServiceProvider;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.utils.concurrent.Futures;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Primitive executor.
 */
public class PrimitiveFactory<P extends PrimitiveProxy> {
  private final PartitionService partitionService;
  private final ServiceType serviceType;
  private final BiFunction<ServiceId, PartitionClient, P> primitiveFactory;

  public PrimitiveFactory(
      PartitionService partitionService,
      ServiceType serviceType,
      BiFunction<ServiceId, PartitionClient, P> primitiveFactory) {
    this.partitionService = partitionService;
    this.serviceType = serviceType;
    this.primitiveFactory = primitiveFactory;
  }

  /**
   * Returns the primitive name for the given ID.
   *
   * @param id the primitive ID
   * @return the primitive name
   */
  public String getPrimitiveName(PrimitiveId id) {
    String name = id.getName();
    return !Strings.isNullOrEmpty(name) ? name : null;
  }

  /**
   * Converts the given primitive Id to a protocol.
   *
   * @param protocol the protocol configuration
   * @return the primitive protocol
   */
  private PartitionGroup getPartitionGroup(Any protocol) {
    String url = protocol.getTypeUrl();
    String type = url.substring(url.lastIndexOf('/') + 1);
    return partitionService.getPartitionGroup(type);
  }

  /**
   * Returns the service ID for the given primitive ID.
   *
   * @param id the primitive ID
   * @return the service ID
   */
  private ServiceId getServiceId(PrimitiveId id) {
    return ServiceId.newBuilder()
        .setName(id.getName())
        .setType(serviceType.name())
        .build();
  }

  /**
   * Returns the partition group for the given ID.
   *
   * @param id the ID for which to return the partition group
   * @return the partition group name
   */
  public String getPartitionGroup(PrimitiveId id) {
    return getPartitionGroup(id.getProtocol()).name();
  }

  /**
   * Returns the client for the given primitive.
   *
   * @param id the primitive ID
   * @return the client for the given primitive
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<PrimitiveClient> getClient(PrimitiveId id) {
    PartitionGroup partitionGroup = getPartitionGroup(id.getProtocol());
    try {
      Object protocol = id.getProtocol().unpack(partitionGroup.type().getProtocolType());
      return ((ServiceProvider<Object>) partitionGroup).createService(id.getName(), protocol);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  /**
   * Returns a proxy for the given partition.
   *
   * @param id           the primitive ID
   * @param partitionKey the partition key
   * @return the primitive
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(PrimitiveId id, String partitionKey) {
    return getClient(id).thenApply(client -> {
      PartitionId partitionId = client.getPartitionId(partitionKey);
      return Pair.of(partitionId, primitiveFactory.apply(getServiceId(id), client.getPartition(partitionId)));
    });
  }

  /**
   * Returns a proxy for the given partition.
   *
   * @param id          the primitive ID
   * @param partitionId the partition ID
   * @return the primitive
   */
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(PrimitiveId id, int partitionId) {
    return getPrimitive(id, PartitionId.newBuilder()
        .setPartition(partitionId)
        .setGroup(getPartitionGroup(id))
        .build());
  }

  /**
   * Returns a proxy for the given partition.
   *
   * @param id          the primitive ID
   * @param partitionId the partition ID
   * @return the primitive
   */
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(PrimitiveId id, PartitionId partitionId) {
    return getClient(id).thenApply(client -> Pair.of(partitionId, primitiveFactory.apply(getServiceId(id), client.getPartition(partitionId))));
  }

  /**
   * Returns a primitive proxy by ID.
   *
   * @param id the primitive ID
   * @return the primitive proxy
   */
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(PrimitiveId id) {
    return getPrimitive(id, getPrimitiveName(id));
  }

  /**
   * Returns a map of all partitions for the given primitive.
   *
   * @param id the primitive ID
   * @return the primitive partitions
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Map<PartitionId, P>> getPrimitives(PrimitiveId id) {
    PartitionGroup partitionGroup = getPartitionGroup(id.getProtocol());
    try {
      Object protocol = id.getProtocol().unpack(partitionGroup.type().getProtocolType());
      return ((ServiceProvider<Object>) partitionGroup).createService(id.getName(), protocol)
          .thenApply(client -> client.getPartitionIds().stream()
              .map(partitionId -> Maps.immutableEntry(partitionId, primitiveFactory.apply(getServiceId(id), client.getPartition(partitionId))))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  /**
   * Returns a map of all partitions for the given keys.
   *
   * @param id   the primitive ID
   * @param keys the keys to partition
   * @return the primitive partitions
   */
  public CompletableFuture<Map<PartitionId, Pair<Collection<String>, P>>> getPrimitives(PrimitiveId id, Collection<String> keys) {
    return getClient(id).thenApply(client -> {
      Map<PartitionId, Collection<String>> partitions = new HashMap<>();
      for (String key : keys) {
        PartitionId partitionId = client.getPartitionId(key);
        partitions.computeIfAbsent(partitionId, i -> new LinkedList<>()).add(key);
      }
      return partitions.entrySet().stream()
          .map(e -> Maps.immutableEntry(e.getKey(), Pair.of(e.getValue(), primitiveFactory.apply(getServiceId(id), client.getPartition(e.getKey())))))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    });
  }
}
