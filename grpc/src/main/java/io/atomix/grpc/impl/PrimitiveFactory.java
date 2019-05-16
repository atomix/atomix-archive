package io.atomix.grpc.impl;

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
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.Atomix;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.atomix.grpc.protocol.MultiPrimaryProtocol;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Primitive executor.
 */
public class PrimitiveFactory<P extends PrimitiveProxy, I extends Message> {
  private final Atomix atomix;
  private final ServiceType serviceType;
  private final BiFunction<ServiceId, PartitionClient, P> primitiveFactory;
  private final PrimitiveIdDescriptor<I> primitiveIdDescriptor;
  private final PrimitiveManagementService managementService;

  public PrimitiveFactory(
      Atomix atomix,
      ServiceType serviceType,
      BiFunction<ServiceId, PartitionClient, P> primitiveFactory,
      PrimitiveIdDescriptor<I> primitiveIdDescriptor) {
    this.atomix = atomix;
    this.serviceType = serviceType;
    this.primitiveFactory = primitiveFactory;
    this.primitiveIdDescriptor = primitiveIdDescriptor;
    this.managementService = new PartialPrimitiveManagementService(atomix);
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
      return io.atomix.protocols.raft.MultiRaftProtocol.builder(group).build();
    }

    if (primitiveIdDescriptor.hasMultiPrimaryProtocol(id)) {
      // TODO
      throw new UnsupportedOperationException();
    }

    if (primitiveIdDescriptor.hasDistributedLogProtocol(id)) {
      String group = primitiveIdDescriptor.getDistributedLogProtocol(id).getGroup();
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
   * Creates a new session for the given primitive.
   *
   * @return a future to be completed with the new session ID
   */
  public CompletableFuture<Long> createSession() {
    return atomix.getSessionIdService().nextSessionId().thenApply(SessionId::id);
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
    return protocol.createService(getPrimitiveName(id), managementService.getPartitionService())
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
  public CompletableFuture<Pair<PartitionId, P>> getPrimitive(I id, PartitionId partitionId) {
    ServiceProtocol protocol = toProtocol(id);
    return protocol.createService(getPrimitiveName(id), managementService.getPartitionService())
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
    return protocol.createService(getPrimitiveName(id), managementService.getPartitionService())
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
    return protocol.createService(getPrimitiveName(id), managementService.getPartitionService())
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

  private static class PartialPrimitiveManagementService implements PrimitiveManagementService {
    private final Atomix atomix;

    PartialPrimitiveManagementService(Atomix atomix) {
      this.atomix = atomix;
    }

    @Override
    public ClusterMembershipService getMembershipService() {
      return atomix.getMembershipService();
    }

    @Override
    public ClusterCommunicationService getCommunicationService() {
      return atomix.getCommunicationService();
    }

    @Override
    public ClusterEventService getEventService() {
      return atomix.getEventService();
    }

    @Override
    public PartitionService getPartitionService() {
      return atomix.getPartitionService();
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
      return atomix.getPrimitiveTypes();
    }

    @Override
    public PrimitiveProtocolTypeRegistry getProtocolTypeRegistry() {
      return atomix.getProtocolTypes();
    }

    @Override
    public PartitionGroupTypeRegistry getPartitionGroupTypeRegistry() {
      return atomix.getPartitionGroupTypes();
    }

    @Override
    public SessionIdService getSessionIdService() {
      return atomix.getSessionIdService();
    }

    @Override
    public ThreadContextFactory getThreadFactory() {
      return atomix.getThreadFactory();
    }
  }

  /**
   * Primitive identifier descriptor.
   */
  public interface PrimitiveIdDescriptor<I extends Message> {

    /**
     * Returns the name for the given ID.
     *
     * @param id the primitive ID
     * @return the primitive name
     */
    String getName(I id);

    /**
     * Returns a boolean indicating whether the given ID contains a multi-Raft protocol configuration.
     *
     * @param id the ID to check
     * @return indicates whether the given ID contains a multi-Raft protocol configuration
     */
    boolean hasMultiRaftProtocol(I id);

    /**
     * Returns the multi-Raft protocol configuration for the given ID.
     *
     * @param id the primitive ID
     * @return the multi-Raft protocol configuration for the given ID
     */
    MultiRaftProtocol getMultiRaftProtocol(I id);

    /**
     * Returns a boolean indicating whether the given ID contains a multi-primary protocol configuration.
     *
     * @param id the ID to check
     * @return indicates whether the given ID contains a multi-primary protocol configuration
     */
    boolean hasMultiPrimaryProtocol(I id);

    /**
     * Returns the multi-primary protocol configuration for the given ID.
     *
     * @param id the primitive ID
     * @return the multi-primary protocol configuration for the given ID
     */
    MultiPrimaryProtocol getMultiPrimaryProtocol(I id);

    /**
     * Returns a boolean indicating whether the given ID contains a distributed log protocol configuration.
     *
     * @param id the ID to check
     * @return indicates whether the given ID contains a distributed log protocol configuration
     */
    boolean hasDistributedLogProtocol(I id);

    /**
     * Returns the distributed log protocol configuration for the given ID.
     *
     * @param id the primitive ID
     * @return the distributed log protocol configuration for the given ID
     */
    DistributedLogProtocol getDistributedLogProtocol(I id);
  }
}
