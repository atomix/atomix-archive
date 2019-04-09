/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.grpc.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.core.Atomix;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionType;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.grpc.election.AnointRequest;
import io.atomix.grpc.election.AnointResponse;
import io.atomix.grpc.election.ElectionEvent;
import io.atomix.grpc.election.ElectionId;
import io.atomix.grpc.election.ElectionServiceGrpc;
import io.atomix.grpc.election.EvictRequest;
import io.atomix.grpc.election.EvictResponse;
import io.atomix.grpc.election.GetLeadershipRequest;
import io.atomix.grpc.election.GetLeadershipResponse;
import io.atomix.grpc.election.Leadership;
import io.atomix.grpc.election.PromoteRequest;
import io.atomix.grpc.election.PromoteResponse;
import io.atomix.grpc.election.RunRequest;
import io.atomix.grpc.election.RunResponse;
import io.atomix.grpc.election.WithdrawRequest;
import io.atomix.grpc.election.WithdrawResponse;
import io.grpc.stub.StreamObserver;

/**
 * Election service implementation.
 */
public class ElectionServiceImpl extends ElectionServiceGrpc.ElectionServiceImplBase {
  private final PrimitiveExecutor<LeaderElection<String>, AsyncLeaderElection<String>> executor;

  public ElectionServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, LeaderElectionType.instance(), LeaderElection::async);
  }

  private Leadership toLeadership(io.atomix.core.election.Leadership<String> leadership) {
    Leadership.Builder builder = Leadership.newBuilder()
        .addAllCandidates(leadership.candidates());
    if (leadership.leader() != null) {
      builder.setLeader(leadership.leader().id());
      builder.setTerm(leadership.leader().term());
    }
    return builder.build();
  }

  @Override
  public void getLeadership(GetLeadershipRequest request, StreamObserver<GetLeadershipResponse> responseObserver) {
    executor.execute(request, GetLeadershipResponse::getDefaultInstance, responseObserver,
        election -> election.getLeadership()
            .thenApply(leadership -> GetLeadershipResponse.newBuilder()
                .setLeadership(toLeadership(leadership))
                .build()));
  }

  @Override
  public void run(RunRequest request, StreamObserver<RunResponse> responseObserver) {
    executor.execute(request, RunResponse::getDefaultInstance, responseObserver,
        election -> election.run(request.getCandidate())
            .thenApply(leadership -> RunResponse.newBuilder()
                .setLeadership(toLeadership(leadership))
                .build()));
  }

  @Override
  public void withdraw(WithdrawRequest request, StreamObserver<WithdrawResponse> responseObserver) {
    executor.execute(request, WithdrawResponse::getDefaultInstance, responseObserver,
        election -> election.withdraw(request.getCandidate())
            .thenApply(v -> WithdrawResponse.newBuilder().build()));
  }

  @Override
  public void anoint(AnointRequest request, StreamObserver<AnointResponse> responseObserver) {
    executor.execute(request, AnointResponse::getDefaultInstance, responseObserver,
        election -> election.anoint(request.getCandidate())
            .thenApply(succeeded -> AnointResponse.newBuilder().setSucceeded(succeeded).build()));
  }

  @Override
  public void promote(PromoteRequest request, StreamObserver<PromoteResponse> responseObserver) {
    executor.execute(request, PromoteResponse::getDefaultInstance, responseObserver,
        election -> election.promote(request.getCandidate())
            .thenApply(succeeded -> PromoteResponse.newBuilder().setSucceeded(succeeded).build()));
  }

  @Override
  public void evict(EvictRequest request, StreamObserver<EvictResponse> responseObserver) {
    executor.execute(request, EvictResponse::getDefaultInstance, responseObserver,
        election -> election.evict(request.getCandidate())
            .thenApply(v -> EvictResponse.newBuilder().build()));
  }

  @Override
  public StreamObserver<ElectionId> listen(StreamObserver<ElectionEvent> responseObserver) {
    Map<ElectionId, LeadershipEventListener<String>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<ElectionId>() {
      @Override
      public void onNext(ElectionId id) {
        if (executor.isValidId(id, ElectionEvent::getDefaultInstance, responseObserver)) {
          LeadershipEventListener<String> listener = event -> {
            responseObserver.onNext(ElectionEvent.newBuilder()
                .setId(id)
                .setLeadership(toLeadership(event.newLeadership()))
                .build());
          };
          listeners.put(id, listener);
          executor.getPrimitive(id).thenAccept(election -> election.addListener(listener));
        }
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(election -> election.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(election -> election.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
