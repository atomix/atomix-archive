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
package io.atomix.core.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryTerm;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Leader elector based primary election.
 */
public class PrimitivePrimaryElection implements PrimaryElection {
  private final PartitionId partitionId;
  private final AsyncLeaderElection<GroupMember> election;
  private final Map<Consumer<PrimaryElectionEvent>, LeadershipEventListener<GroupMember>> eventListeners = new ConcurrentHashMap<>();

  PrimitivePrimaryElection(PartitionId partitionId, AsyncLeaderElection<GroupMember> election) {
    this.partitionId = checkNotNull(partitionId);
    this.election = election;
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
    return election.run(member)
        .thenApply(leadership -> PrimaryTerm.newBuilder()
            .setTerm(leadership.leader().term())
            .setPrimary(leadership.leader().id())
            .addAllCandidates(leadership.candidates())
            .build());
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return election.getLeadership()
        .thenApply(leadership -> PrimaryTerm.newBuilder()
            .setTerm(leadership.leader().term())
            .setPrimary(leadership.leader().id())
            .addAllCandidates(leadership.candidates())
            .build());
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(Consumer<PrimaryElectionEvent> listener) {
    LeadershipEventListener<GroupMember> eventListener = event -> {
      listener.accept(PrimaryElectionEvent.newBuilder()
          .setTerm(PrimaryTerm.newBuilder()
              .setTerm(event.leadership().leader().term())
              .setPrimary(event.leadership().leader().id())
              .addAllCandidates(event.leadership().candidates())
              .build())
          .build());
    };
    eventListeners.put(listener, eventListener);
    return election.addListener(eventListener);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(Consumer<PrimaryElectionEvent> listener) {
    LeadershipEventListener<GroupMember> eventListener = eventListeners.remove(listener);
    if (eventListener != null) {
      return election.removeListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Closes the election.
   *
   * @return a future to be completed once the election has been closed
   */
  public CompletableFuture<Void> close() {
    return election.close();
  }
}
