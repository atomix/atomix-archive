package io.atomix.primitive.proxy;

import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.ServiceType;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Abstract primitive proxy.
 */
public abstract class AbstractPrimitiveProxy<T> implements PrimitiveProxy {
  private final Context context;
  private final T client;
  private final ServiceId serviceId;
  private final ThreadContext threadContext;

  protected AbstractPrimitiveProxy(Context context) {
    this.context = context;
    this.threadContext = context.threadFactory().createContext();
    this.serviceId = ServiceId.newBuilder()
        .setName(context.name())
        .setType(type().name())
        .build();
    this.client = createClient();
  }

  @Override
  public String name() {
    return context.name();
  }

  @Override
  public ServiceType type() {
    return context.type();
  }

  @Override
  public ThreadContext context() {
    return threadContext;
  }

  /**
   * Creates a new proxy client.
   *
   * @return a new proxy client
   */
  protected abstract T createClient();

  /**
   * Returns the primitive client.
   *
   * @return the primitive client
   */
  protected T getClient() {
    return client;
  }

  /**
   * Returns the service ID.
   *
   * @return the service ID
   */
  protected ServiceId serviceId() {
    return serviceId;
  }

  @Override
  public PartitionId partitionId() {
    return context.partition().id();
  }

  /**
   * Returns the proxy partition.
   *
   * @return the proxy partition
   */
  protected Partition getPartition() {
    return context.partition();
  }

  /**
   * Returns the partition client.
   *
   * @return the partition client
   */
  protected PartitionClient getPartitionClient() {
    return getPartition().getClient();
  }
}
