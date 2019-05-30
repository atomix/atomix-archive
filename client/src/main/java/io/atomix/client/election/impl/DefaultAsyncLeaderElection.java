/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.client.election.impl;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.common.base.Strings;
import io.atomix.api.election.AnointRequest;
import io.atomix.api.election.AnointResponse;
import io.atomix.api.election.CloseRequest;
import io.atomix.api.election.CloseResponse;
import io.atomix.api.election.CreateRequest;
import io.atomix.api.election.CreateResponse;
import io.atomix.api.election.EnterRequest;
import io.atomix.api.election.EnterResponse;
import io.atomix.api.election.EventRequest;
import io.atomix.api.election.EventResponse;
import io.atomix.api.election.EvictRequest;
import io.atomix.api.election.EvictResponse;
import io.atomix.api.election.GetLeadershipRequest;
import io.atomix.api.election.GetLeadershipResponse;
import io.atomix.api.election.KeepAliveRequest;
import io.atomix.api.election.KeepAliveResponse;
import io.atomix.api.election.LeaderElectionServiceGrpc;
import io.atomix.api.election.PromoteRequest;
import io.atomix.api.election.PromoteResponse;
import io.atomix.api.election.WithdrawRequest;
import io.atomix.api.election.WithdrawResponse;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.election.AsyncLeaderElection;
import io.atomix.client.election.Leader;
import io.atomix.client.election.LeaderElection;
import io.atomix.client.election.Leadership;
import io.atomix.client.election.LeadershipEvent;
import io.atomix.client.election.LeadershipEventListener;
import io.atomix.client.impl.AbstractManagedPrimitive;
import io.grpc.stub.StreamObserver;

/**
 * Distributed resource providing the {@link AsyncLeaderElection} primitive.
 */
public class DefaultAsyncLeaderElection
    extends AbstractManagedPrimitive<LeaderElectionServiceGrpc.LeaderElectionServiceStub, AsyncLeaderElection<String>>
    implements AsyncLeaderElection<String> {
  private volatile CompletableFuture<Long> listenFuture;
  private final Set<LeadershipEventListener<String>> eventListeners = new CopyOnWriteArraySet<>();

  public DefaultAsyncLeaderElection(PrimitiveId id, PrimitiveManagementService managementService, Duration timeout) {
    super(id, LeaderElectionServiceGrpc.newStub(managementService.getChannelFactory().getChannel()), managementService, timeout);
  }

  @Override
  public CompletableFuture<Leadership<String>> run(String identifier) {
    return command(
        (election, header, observer) -> election.enter(EnterRequest.newBuilder()
            .setElectionId(getPrimitiveId())
            .setHeader(header)
            .setCandidateId(identifier)
            .build(), observer), EnterResponse::getHeader)
        .thenApply(response -> new Leadership<>(!Strings.isNullOrEmpty(response.getLeader())
            ? new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp())
            : null,
            response.getCandidatesList()));
  }

  @Override
  public CompletableFuture<Void> withdraw(String identifier) {
    return command(
        (election, header, observer) -> election.withdraw(WithdrawRequest.newBuilder()
            .setElectionId(getPrimitiveId())
            .setHeader(header)
            .setCandidateId(identifier)
            .build(), observer), WithdrawResponse::getHeader)
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> anoint(String identifier) {
    return command(
        (election, header, observer) -> election.anoint(AnointRequest.newBuilder()
            .setElectionId(getPrimitiveId())
            .setHeader(header)
            .setCandidateId(identifier)
            .build(), observer), AnointResponse::getHeader)
        .thenApply(response -> response.getSucceeded());
  }

  @Override
  public CompletableFuture<Void> evict(String identifier) {
    return command(
        (election, header, observer) -> election.evict(EvictRequest.newBuilder()
            .setElectionId(getPrimitiveId())
            .setHeader(header)
            .setCandidateId(identifier)
            .build(), observer), EvictResponse::getHeader)
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> promote(String identifier) {
    return command(
        (election, header, observer) -> election.promote(PromoteRequest.newBuilder()
            .setElectionId(getPrimitiveId())
            .setHeader(header)
            .setCandidateId(identifier)
            .build(), observer), PromoteResponse::getHeader)
        .thenApply(response -> response.getSucceeded());
  }

  @Override
  public CompletableFuture<Leadership<String>> getLeadership() {
    return query(
        (election, header, observer) -> election.getLeadership(GetLeadershipRequest.newBuilder()
            .setElectionId(getPrimitiveId())
            .setHeader(header)
            .build(), observer), GetLeadershipResponse::getHeader)
        .thenApply(response -> new Leadership<>(!Strings.isNullOrEmpty(response.getLeader())
            ? new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp())
            : null,
            response.getCandidatesList()));
  }

  private synchronized CompletableFuture<Void> listen() {
    if (listenFuture == null && !eventListeners.isEmpty()) {
      listenFuture = command(
          (service, header, observer) -> service.events(EventRequest.newBuilder()
              .setElectionId(getPrimitiveId())
              .setHeader(header)
              .build(), observer),
          EventResponse::getHeader,
          new StreamObserver<EventResponse>() {
            @Override
            public void onNext(EventResponse response) {
              LeadershipEvent<String> event = null;
              switch (response.getType()) {
                case CHANGED:
                  event = new LeadershipEvent<>(
                      LeadershipEvent.Type.CHANGED,
                      new Leadership<>(!Strings.isNullOrEmpty(response.getLeader())
                          ? new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp())
                          : null,
                          response.getCandidatesList()));
                  break;
              }
              onEvent(event);
            }

            private void onEvent(LeadershipEvent<String> event) {
              eventListeners.forEach(l -> l.event(event));
            }

            @Override
            public void onError(Throwable t) {
              onCompleted();
            }

            @Override
            public void onCompleted() {
              synchronized (DefaultAsyncLeaderElection.this) {
                listenFuture = null;
              }
              listen();
            }
          });
    }
    return listenFuture.thenApply(v -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(LeadershipEventListener<String> listener) {
    eventListeners.add(listener);
    return listen();
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(LeadershipEventListener<String> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  protected CompletableFuture<Long> openSession(Duration timeout) {
    return this.<CreateResponse>session((service, header, observer) -> service.create(CreateRequest.newBuilder()
        .setElectionId(getPrimitiveId())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), observer))
        .thenApply(response -> response.getHeader().getSessionId());
  }

  @Override
  protected CompletableFuture<Boolean> keepAlive() {
    return this.<KeepAliveResponse>session((service, header, observer) -> service.keepAlive(KeepAliveRequest.newBuilder()
        .setElectionId(getPrimitiveId())
        .build(), observer))
        .thenApply(response -> true);
  }

  @Override
  protected CompletableFuture<Void> close(boolean delete) {
    return this.<CloseResponse>session((service, header, observer) -> service.close(CloseRequest.newBuilder()
        .setElectionId(getPrimitiveId())
        .setDelete(delete)
        .build(), observer))
        .thenApply(v -> null);
  }

  @Override
  public LeaderElection<String> sync(Duration operationTimeout) {
    return new BlockingLeaderElection<>(this, operationTimeout.toMillis());
  }
}