package io.atomix.server;

import io.atomix.cluster.ClusterService;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.utils.Version;
import io.atomix.utils.concurrent.ThreadContextFactory;

/**
 * Atomix server service.
 */
public interface AtomixService extends ClusterService {

  /**
   * Returns the Atomix version.
   *
   * @return the Atomix version
   */
  Version getVersion();

  /**
   * Returns the protocol type registry.
   *
   * @return the protocol type registry
   */
  PrimitiveProtocolTypeRegistry getProtocolTypes();

  /**
   * Returns the partition group type registry.
   *
   * @return the partition group type registry
   */
  PartitionGroupTypeRegistry getPartitionGroupTypes();

  /**
   * Returns the Atomix primitive thread factory.
   *
   * @return the primitive thread context factory
   */
  ThreadContextFactory getThreadFactory();

  /**
   * Returns the partition service.
   * <p>
   * The partition service is responsible for managing the lifecycle of primitive partitions and can provide information
   * about active partition groups and partitions in the cluster.
   *
   * @return the partition service
   */
  PartitionService getPartitionService();

}
