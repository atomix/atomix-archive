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
package io.atomix.primitive.partition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

/**
 * Test primary election.
 */
public class TestPrimaryElection implements PrimaryElection {
  private final PartitionId partitionId;
  private long counter;
  private PrimaryTerm term;
  private final List<GroupMember> candidates = new ArrayList<>();
  private final Set<Consumer<PrimaryElectionEvent>> listeners = Sets.newConcurrentHashSet();

  public TestPrimaryElection(PartitionId partitionId) {
    this.partitionId = partitionId;
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
    candidates.add(member);
    if (term == null) {
      term = PrimaryTerm.newBuilder()
          .setTerm(++counter)
          .setPrimary(member)
          .build();
      listeners.forEach(l -> l.accept(PrimaryElectionEvent.newBuilder()
          .setPartitionId(partitionId)
          .setTerm(term)
          .build()));
    } else {
      term = PrimaryTerm.newBuilder(term)
          .addAllCandidates(candidates.stream()
              .filter(candidate -> !candidate.getMemberId().equals(term.getPrimary().getMemberId()))
              .collect(Collectors.toList()))
          .build();
      listeners.forEach(l -> l.accept(PrimaryElectionEvent.newBuilder()
          .setPartitionId(partitionId)
          .setTerm(term)
          .build()));
    }
    return CompletableFuture.completedFuture(term);
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return CompletableFuture.completedFuture(term);
  }

  @Override
  public void addListener(Consumer<PrimaryElectionEvent> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeListener(Consumer<PrimaryElectionEvent> listener) {
    listeners.remove(listener);
  }
}
