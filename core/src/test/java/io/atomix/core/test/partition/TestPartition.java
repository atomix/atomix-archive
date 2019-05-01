/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.test.partition;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.atomix.cluster.MemberId;
import io.atomix.core.test.protocol.TestPartitionClient;
import io.atomix.core.test.protocol.TestStateMachine;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Test partition.
 */
public class TestPartition implements Partition {
  private final PartitionId partitionId;
  private volatile PartitionClient client;

  public TestPartition(PartitionId partitionId) {
    this.partitionId = partitionId;
  }

  @Override
  public PartitionId id() {
    return partitionId;
  }

  @Override
  public long term() {
    return 1;
  }

  @Override
  public MemberId primary() {
    return null;
  }

  @Override
  public Collection<MemberId> backups() {
    MemberId leader = primary();
    if (leader == null) {
      return members();
    }
    return members().stream()
        .filter(m -> !m.equals(leader))
        .collect(Collectors.toSet());
  }

  @Override
  public Collection<MemberId> members() {
    return Collections.emptyList();
  }

  @Override
  public PartitionClient getClient() {
    return client;
  }

  CompletableFuture<Void> join(PartitionManagementService managementService) {
    this.client = new TestPartitionClient(TestStateMachine.getInstance(partitionId, managementService));
    return CompletableFuture.completedFuture(null);
  }

  CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitionId", id())
        .toString();
  }
}
