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

import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.collect.Sets;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.ManagedPrimaryElection;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.impl.SessionMetadata;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Leader elector based primary election.
 */
public class DefaultPrimaryElection implements ManagedPrimaryElection {
  private final PartitionId partitionId;
  private final long sessionId = new Random(System.currentTimeMillis()).nextLong();
  private final SessionClient proxy;
  private final PrimaryElectionService service;
  private final Set<Consumer<PrimaryElectionEvent>> listeners = Sets.newCopyOnWriteArraySet();
  private final Consumer<PrimaryElectionEvent> eventListener;
  private final AtomicBoolean started = new AtomicBoolean();

  public DefaultPrimaryElection(PartitionId partitionId, SessionClient proxy, PrimaryElectionService service) {
    this.partitionId = checkNotNull(partitionId);
    this.proxy = proxy;
    this.service = service;
    this.eventListener = event -> {
      if (event.getPartitionId().equals(partitionId)) {
        listeners.forEach(l -> l.accept(event));
      }
    };
    service.addListener(eventListener);
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
    return proxy.execute(
        PrimaryElectorOperations.ENTER,
        SessionMetadata.newBuilder()
            .setSessionId(sessionId)
            .build(),
        EnterRequest.newBuilder()
            .setPartitionId(partitionId)
            .setMember(member)
            .build(),
        EnterRequest::toByteString,
        EnterResponse::parseFrom)
        .thenApply(response -> response.getRight().getTerm());
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return proxy.execute(
        PrimaryElectorOperations.GET_TERM,
        SessionMetadata.newBuilder()
            .setSessionId(sessionId)
            .build(),
        GetTermRequest.newBuilder()
            .setPartitionId(partitionId)
            .build(),
        GetTermRequest::toByteString,
        GetTermResponse::parseFrom)
        .thenApply(response -> response.getRight().getTerm());
  }

  @Override
  public synchronized void addListener(Consumer<PrimaryElectionEvent> listener) {
    listeners.add(checkNotNull(listener));
  }

  @Override
  public synchronized void removeListener(Consumer<PrimaryElectionEvent> listener) {
    listeners.remove(checkNotNull(listener));
  }

  @Override
  public CompletableFuture<PrimaryElection> start() {
    started.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    service.removeListener(eventListener);
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
