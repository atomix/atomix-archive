package io.atomix.primitive.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.collect.Maps;
import io.atomix.primitive.PrimitiveClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.service.ServiceClient;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default primitive client implementation.
 */
public class DefaultPrimitiveClient implements PrimitiveClient {
  private final List<PartitionId> partitionIds = new CopyOnWriteArrayList<>();
  private final Map<Integer, ServiceClient> intPartitions = Maps.newConcurrentMap();
  private final Map<PartitionId, ServiceClient> partitions = Maps.newConcurrentMap();
  private final List<ServiceClient> sortedPartitions = new CopyOnWriteArrayList<>();
  private final Partitioner<String> partitioner;

  public DefaultPrimitiveClient(
      Map<PartitionId, ServiceClient> partitions,
      Partitioner<String> partitioner) {
    this.partitioner = checkNotNull(partitioner, "partitioner cannot be null");
    partitions.forEach((partitionId, partition) -> {
      this.partitionIds.add(partitionId);
      this.partitions.put(partitionId, partition);
      this.intPartitions.put(partitionId.getPartition(), partition);
      this.sortedPartitions.add(partition);
    });
  }

  @Override
  public Collection<ServiceClient> getPartitions() {
    return sortedPartitions;
  }

  @Override
  public Collection<PartitionId> getPartitionIds() {
    return partitions.keySet();
  }

  @Override
  public ServiceClient getPartition(int partitionId) {
    return intPartitions.get(partitionId);
  }

  @Override
  public ServiceClient getPartition(PartitionId partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public PartitionId getPartitionId(String key) {
    return partitioner.partition(key, partitionIds);
  }
}
