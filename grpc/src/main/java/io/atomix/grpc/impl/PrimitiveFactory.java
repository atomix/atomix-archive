package io.atomix.grpc.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
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
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.ServiceType;
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
  private final Function<PrimitiveProxy.Context, P> primitiveFactory;
  private final PrimitiveIdDescriptor<I> primitiveIdDescriptor;
  private final PrimitiveManagementService managementService;

  public PrimitiveFactory(
      Atomix atomix,
      ServiceType serviceType,
      Function<PrimitiveProxy.Context, P> primitiveFactory,
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
  private ProxyProtocol toProtocol(I id) {
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
      return io.atomix.protocols.log.DistributedLogProtocol.builder(group).build();
    }
    return null;
  }

  /**
   * Returns the partition group for the given ID.
   *
   * @param id the ID for which to return the partition group
   * @return the partition group name
   */
  public String getPartitionGroup(I id) {
    ProxyProtocol protocol = toProtocol(id);
    return protocol != null ? protocol.group() : null;
  }

  /**
   * Returns the partition ID for the given key.
   *
   * @param id  the primitive ID
   * @param key the partition key
   * @return the partition ID for the given key
   */
  public PartitionId getPartitionId(I id, String key) {
    return atomix.getPartitionService()
        .getPartitionGroup(toProtocol(id))
        .getPartition(key)
        .id();
  }

  /**
   * Returns the collection of partition IDs for the given primitive.
   *
   * @param id the primitive ID
   * @return the partition IDs for the given primitive
   */
  public Collection<PartitionId> getPartitionIds(I id) {
    return atomix.getPartitionService()
        .getPartitionGroup(toProtocol(id))
        .getPartitionIds();
  }

  /**
   * Returns the collection of partition IDs for the given keys.
   *
   * @param id   the primitive ID
   * @param keys the keys by which to filter partitions
   * @return the partition IDs for the given primitive
   */
  public Map<PartitionId, Collection<String>> getPartitionIds(I id, Collection<String> keys) {
    PartitionGroup partitionGroup = atomix.getPartitionService().getPartitionGroup(toProtocol(id));
    Map<PartitionId, Collection<String>> partitions = new HashMap<>();
    keys.forEach(key -> partitions.computeIfAbsent(partitionGroup.getPartition(key).id(), i -> new HashSet<>()).add(key));
    return partitions;
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
   * @param name        the primitive name
   * @param partitionId the partition ID
   * @return the primitive
   */
  public P getPrimitive(String name, PartitionId partitionId) {
    return primitiveFactory.apply(new PrimitiveProxy.Context(
        name,
        serviceType,
        managementService.getPartitionService()
            .getPartitionGroup(partitionId.getGroup())
            .getPartition(partitionId),
        managementService.getThreadFactory()));
  }

  /**
   * Returns a primitive proxy by ID.
   *
   * @param id the primitive ID
   * @return the primitive proxy
   */
  public P getPrimitive(I id) {
    String name = getPrimitiveName(id);
    ProxyProtocol protocol = toProtocol(id);
    return getPrimitive(name, atomix.getPartitionService().getPartitionGroup(protocol).getPartition(name).id());
  }

  /**
   * Returns a collection of proxies for the given ID.
   *
   * @param id the ID for which to return proxies
   * @return a collection of proxies for the given ID
   */
  public Map<PartitionId, P> getPrimitives(I id) {
    String name = getPrimitiveName(id);
    ProxyProtocol protocol = toProtocol(id);
    return atomix.getPartitionService().getPartitionGroup(protocol)
        .getPartitions()
        .stream()
        .map(partition -> Pair.of(partition.id(), getPrimitive(name, partition.id())))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
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
