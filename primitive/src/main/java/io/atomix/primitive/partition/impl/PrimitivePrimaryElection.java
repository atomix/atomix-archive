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
package io.atomix.primitive.partition.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

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
  private final PrimaryElector elector;
  private final Map<Consumer<PrimaryElectionEvent>, Consumer<PrimaryElectionEvent>> eventListeners = new ConcurrentHashMap<>();

  PrimitivePrimaryElection(PartitionId partitionId, PrimaryElector elector) {
    this.partitionId = checkNotNull(partitionId);
    this.elector = elector;
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
    return elector.enter(partitionId, member);
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return elector.getTerm(partitionId);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(Consumer<PrimaryElectionEvent> listener) {
    Consumer<PrimaryElectionEvent> eventListener = event -> {
      if (event.getPartitionId().equals(partitionId)) {
        listener.accept(event);
      }
    };
    eventListeners.put(listener, eventListener);
    return elector.addListener(eventListener);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(Consumer<PrimaryElectionEvent> listener) {
    Consumer<PrimaryElectionEvent> eventListener = eventListeners.remove(listener);
    if (eventListener != null) {
      return elector.removeListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }
}
