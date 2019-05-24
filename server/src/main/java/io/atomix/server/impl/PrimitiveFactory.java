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
import com.google.protobuf.Message;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.service.impl.ServiceId;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Primitive executor.
 */
public class PrimitiveFactory<P extends PrimitiveProxy, I extends Message> {
  private final PartitionService partitionService;
  private final ServiceType serviceType;
  private final BiFunction<ServiceId, PartitionClient, P> primitiveFactory;
  private final PrimitiveIdDescriptor<I> primitiveIdDescriptor;

  public PrimitiveFactory(
      PartitionService partitionService,
      ServiceType serviceType,
      BiFunction<ServiceId, PartitionClient, P> primitiveFactory,
      PrimitiveIdDescriptor<I> primitiveIdDescriptor) {
    this.partitionService = partitionService;
    this.serviceType = serviceType;
    this.primitiveFactory = primitiveFactory;
    this.primitiveIdDescriptor = primitiveIdDescriptor;
  }

  /**
   * Returns a boolean indicating whether the given ID has a name.
   *
   * @param id the ID to check
   * @return indicates whether the given ID has a name
   */
  public boolean hasPrimitiveName(I id) {
    return getPrimitiveName(id) != null;
  }

  /**
   * Returns the primitive name for the given ID.
   *
   * @param id the primitive ID
   * @return the primitive name
   */
  public String getPrimitiveName(I id) {
    String name = primitiveIdDescriptor.getName(id);
    return !Strings.isNullOrEmpty(name) ? name : null;
  }

  /**
   * Returns a boolean indicating whether the given ID has a protocol configuration.
   *
   * @param id the ID to check
   * @return indicates whether the given ID has a protocol configuration
   */
  public boolean hasProtocol(I id) {
    return primitiveIdDescriptor.hasMultiRaftProtocol(id)
        || primitiveIdDescriptor.hasMultiPrimaryProtocol(id)
        || primitiveIdDescriptor.hasDistributedLogProtocol(id);
  }

  /**
   * Converts the given primitive Id to a protocol.
   *
   * @param id the primitive ID
   * @return the primitive protocol
   */
  private ServiceProtocol toProtocol(I id) {
    if (primitiveIdDescriptor.hasMultiRaftProtocol(id)) {
      String group = primitiveIdDescriptor.getMultiRaftProtocol(id).getGroup();
      if (Strings.isNullOrEmpty(group)) {
        group = partitionService.getPartitionGroup(io.atomix.protocols.raft.MultiRaftProtocol.TYPE)
            .name();
      }
      return io.atomix.protocols.raft.MultiRaftProtocol.builder(group).build();
    }

    if (primitiveIdDescriptor.hasMultiPrimaryProtocol(id)) {
      // TODO
      throw new UnsupportedOperationException();
    }

    if (primitiveIdDescriptor.hasDistributedLogProtocol(id)) {
      String group = primitiveIdDescriptor.getDistributedLogProtocol(id).getGroup();
      if (Strings.isNullOrEmpty(group)) {
        group = partitionService.getPartitionGroup(io.atomix.protocols.log.DistributedLogProtocol.TYPE)
            .name();
      }
      return io.atomix.protocols.log.DistributedLogProtocol.builder(group)
          .withNumPartitions(primitiveIdDescriptor.getDistributedLogProtocol(id).getPartitions())
          .withReplicationFactor(primitiveIdDescriptor.getDistributedLogProtocol(id).getReplicationFactor())
          .build();
    }
    return null;
  }

  /**
   * Returns the service ID for the given primitive ID.
   *
   * @param id the primitive ID
   * @return the service ID
   */
  private ServiceId getServiceId(I id) {
    return ServiceId.newBuilder()
        .setName(primitiveIdDescriptor.getName(id))
        .setType(serviceType.name())
        .build();
  }

  /**
   * Returns the partition group for the given ID.
   *
   * @param id the ID for which to return the partition group
   * @return the partition group name
   */
  public String getPartitionGroup(I id) {
    ServiceProtocol protocol = toProtocol(id);
    return protocol != null ? protocol.group() : null;
  }

  /**
   * Returns a proxy for the given partition.
   *
   * @param id           the primitive ID
   * @param partitionKey the partition key
   * @return the primitive
   */
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(I id, String partitionKey) {
    ServiceProtocol protocol = toProtocol(id);
    return protocol.createService(getPrimitiveName(id), partitionService)
        .thenApply(client -> {
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
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(I id, int partitionId) {
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
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(I id, PartitionId partitionId) {
    ServiceProtocol protocol = toProtocol(id);
    return protocol.createService(getPrimitiveName(id), partitionService)
        .thenApply(client -> Pair.of(partitionId, primitiveFactory.apply(getServiceId(id), client.getPartition(partitionId))));
  }

  /**
   * Returns a primitive proxy by ID.
   *
   * @param id the primitive ID
   * @return the primitive proxy
   */
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(I id) {
    return getPrimitive(id, getPrimitiveName(id));
  }

  /**
   * Returns a map of all partitions for the given primitive.
   *
   * @param id the primitive ID
   * @return the primitive partitions
   */
  public CompletableFuture<Map<PartitionId, P>> getPrimitives(I id) {
    ServiceProtocol protocol = toProtocol(id);
    return protocol.createService(getPrimitiveName(id), partitionService)
        .thenApply(client -> client.getPartitionIds().stream()
            .map(partitionId -> Maps.immutableEntry(partitionId, primitiveFactory.apply(getServiceId(id), client.getPartition(partitionId))))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  /**
   * Returns a map of all partitions for the given keys.
   *
   * @param id   the primitive ID
   * @param keys the keys to partition
   * @return the primitive partitions
   */
  public CompletableFuture<Map<PartitionId, Pair<Collection<String>, P>>> getPrimitives(I id, Collection<String> keys) {
    ServiceProtocol protocol = toProtocol(id);
    return protocol.createService(getPrimitiveName(id), partitionService)
        .thenApply(client -> {
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
