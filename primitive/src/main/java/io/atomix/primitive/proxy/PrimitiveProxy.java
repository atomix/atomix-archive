package io.atomix.primitive.proxy;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.ServiceType;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;

/**
 * Primitive proxy.
 */
public interface PrimitiveProxy {

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  String name();

  /**
   * Returns the service type.
   *
   * @return the service type
   */
  ServiceType type();

  /**
   * Returns the partition ID.
   *
   * @return the partition ID
   */
  PartitionId partitionId();

  /**
   * Returns the proxy thread context.
   *
   * @return the proxy thread context
   */
  ThreadContext context();

  /**
   * Deletes the primitive.
   *
   * @return a future to be completed once the primitive has been deleted
   */
  CompletableFuture<Void> delete();

  /**
   * Proxy context.
   */
  class Context {
    private final String name;
    private final ServiceType type;
    private final Partition partition;
    private final ThreadContextFactory threadFactory;

    public Context(String name, ServiceType type, Partition partition, ThreadContextFactory threadFactory) {
      this.name = name;
      this.type = type;
      this.partition = partition;
      this.threadFactory = threadFactory;
    }

    /**
     * Returns the primitive name.
     *
     * @return the primitive name
     */
    public String name() {
      return name;
    }

    /**
     * Returns the service type.
     *
     * @return the service type
     */
    public ServiceType type() {
      return type;
    }

    /**
     * Returns the partition.
     *
     * @return the partition
     */
    public Partition partition() {
      return partition;
    }

    /**
     * Returns the primitive thread factory.
     *
     * @return the primitive thread factory
     */
    public ThreadContextFactory threadFactory() {
      return threadFactory;
    }
  }
}
