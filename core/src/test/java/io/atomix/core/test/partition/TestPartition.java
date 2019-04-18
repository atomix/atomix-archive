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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.cluster.MemberId;
import io.atomix.core.test.protocol.TestPartitionClient;
import io.atomix.core.test.protocol.TestStateMachineContext;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.StateMachine;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.impl.PrimitiveSessionClient;
import io.atomix.utils.concurrent.ThreadContextFactory;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Test partition.
 */
public class TestPartition implements Partition {
  private final PartitionId partitionId;
  private final ThreadContextFactory threadContextFactory;

  public TestPartition(
      PartitionId partitionId,
      ThreadContextFactory threadContextFactory) {
    this.partitionId = partitionId;
    this.threadContextFactory = threadContextFactory;
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

  /**
   * Creates a new primitive client.
   *
   * @param name              the primitive name
   * @param type              the primitive type
   * @param managementService the partition management service
   * @param stateMachine      the test state machine
   * @param context           the test state machine context
   * @return the primitive client
   */
  public SessionClient newClient(
      String name,
      PrimitiveType type,
      PrimitiveManagementService managementService,
      StateMachine stateMachine,
      TestStateMachineContext context) {
    return new PrimitiveSessionClient(
        name,
        type,
        Duration.ofSeconds(5),
        new TestPartitionClient(stateMachine, context),
        managementService.getMembershipService(),
        managementService.getCommunicationService(),
        threadContextFactory.createContext());
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("partitionId", id())
        .toString();
  }
}
