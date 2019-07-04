/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.node.management.impl;

import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import io.atomix.api.controller.ControllerServiceGrpc;
import io.atomix.api.controller.PartitionElectionRequest;
import io.atomix.api.controller.PartitionElectionResponse;
import io.atomix.api.controller.PrimaryTerm;
import io.atomix.node.management.ClusterService;
import io.atomix.node.management.ControllerService;
import io.atomix.node.management.PartitionService;
import io.atomix.node.management.PrimaryElectionEvent;
import io.atomix.node.management.PrimaryElectionService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.ThreadService;
import io.grpc.stub.StreamObserver;

/**
 * Leader elector based primary election.
 */
@Component
public class PrimaryElectionServiceImpl implements PrimaryElectionService, Managed {
  @Dependency
  private ControllerService controllerService;
  @Dependency
  private ClusterService clusterService;
  @Dependency
  private PartitionService partitionService;
  @Dependency
  private ThreadService threadService;

  private ControllerServiceGrpc.ControllerServiceStub election;
  private final Set<Consumer<PrimaryElectionEvent>> eventListeners = new CopyOnWriteArraySet<>();
  private final Queue<CompletableFuture<PrimaryTerm>> futures = new LinkedList<>();
  private volatile PrimaryTerm term;

  @Override
  public CompletableFuture<Void> start() {
    election = ControllerServiceGrpc.newStub(controllerService.getChannel());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter() {
    CompletableFuture<PrimaryTerm> future = new CompletableFuture<>();
    synchronized (futures) {
      futures.add(future);
    }

    election.enterElection(PartitionElectionRequest.newBuilder()
        .setPartitionId(partitionService.getPartitionId())
        .setMember(clusterService.getLocalNode().id())
        .build(), new StreamObserver<PartitionElectionResponse>() {
      @Override
      public void onNext(PartitionElectionResponse response) {
        term = response.getTerm();
        Queue<CompletableFuture<PrimaryTerm>> futures;
        synchronized (PrimaryElectionServiceImpl.this) {
          futures = new ArrayDeque<>(PrimaryElectionServiceImpl.this.futures);
          PrimaryElectionServiceImpl.this.futures.clear();
        }
        futures.forEach(future -> future.complete(response.getTerm()));
        PrimaryElectionEvent event = new PrimaryElectionEvent(PrimaryElectionEvent.Type.TERM_CHANGED, response.getTerm());
        eventListeners.forEach(listener -> listener.accept(event));
      }

      @Override
      public void onError(Throwable t) {
        term = null;
      }

      @Override
      public void onCompleted() {

      }
    });
    return future;
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    PrimaryTerm term = this.term;
    if (term == null) {
      synchronized (this) {
        term = this.term;
        if (term == null) {
          CompletableFuture<PrimaryTerm> future = new CompletableFuture<>();
          futures.add(future);
          return future;
        }
      }
    }
    return CompletableFuture.completedFuture(term);
  }

  @Override
  public CompletableFuture<Void> addListener(Consumer<PrimaryElectionEvent> listener) {
    eventListeners.add(listener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(Consumer<PrimaryElectionEvent> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }
}
