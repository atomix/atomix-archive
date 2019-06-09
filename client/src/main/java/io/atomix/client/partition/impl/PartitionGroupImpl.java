package io.atomix.client.partition.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import io.atomix.api.controller.PartitionGroupId;
import io.atomix.client.partition.Partition;
import io.atomix.client.partition.PartitionGroup;

/**
 * Partition group implementation.
 */
public class PartitionGroupImpl implements PartitionGroup {
  private final io.atomix.api.controller.PartitionGroup group;
  private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
  private final List<Integer> partitionIds = new CopyOnWriteArrayList<>();

  public PartitionGroupImpl(io.atomix.api.controller.PartitionGroup group) {
    this.group = group;
    group.getPartitionsList().forEach(partition -> {
      partitions.put(partition.getPartitionId(), new PartitionImpl(partition));
      partitionIds.add(partition.getPartitionId());
    });
    Collections.sort(partitionIds);
  }

  @Override
  public PartitionGroupId id() {
    return group.getId();
  }

  @Override
  public Partition getPartition(int partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public List<Integer> getPartitionIds() {
    return partitionIds;
  }

  @Override
  public Collection<Partition> getPartitions() {
    return partitions.values();
  }
}
