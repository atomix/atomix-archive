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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.atomix.core.Atomix;
import io.atomix.core.election.AsyncLeaderElection;
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
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Election service implementation.
 */
public class ElectionServiceImpl extends ElectionServiceGrpc.ElectionServiceImplBase {
  private final Atomix atomix;

  public ElectionServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(ElectionId id) {
    if (id.hasRaft()) {
      return MultiRaftProtocol.builder(id.getRaft().getGroup())
          .build();
    } else if (id.hasMultiPrimary()) {
      return MultiPrimaryProtocol.builder(id.getMultiPrimary().getGroup())
          .build();
    } else if (id.hasLog()) {
      return DistributedLogProtocol.builder(id.getLog().getGroup())
          .build();
    }
    return null;
  }

  private CompletableFuture<AsyncLeaderElection<String>> getElection(ElectionId id) {
    return atomix.<String>leaderElectionBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(election -> election.async());
  }

  private <T> void run(ElectionId id, Function<AsyncLeaderElection<String>, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getElection(id).whenComplete((election, getError) -> {
      if (getError == null) {
        function.apply(election).whenComplete((result, funcError) -> {
          if (funcError == null) {
            responseObserver.onNext(result);
            responseObserver.onCompleted();
          } else {
            responseObserver.onError(funcError);
            responseObserver.onCompleted();
          }
        });
      } else {
        responseObserver.onError(getError);
        responseObserver.onCompleted();
      }
    });
  }

  private Leadership toLeadership(io.atomix.core.election.Leadership<String> leadership) {
    Leadership.Builder builder = Leadership.newBuilder();
    if (leadership.leader() != null) {
      builder.setLeader(leadership.leader().id());
      builder.setTerm(leadership.leader().term());
    }
    for (int i = 0; i < leadership.candidates().size(); i++) {
      builder.setCandidates(i, leadership.candidates().get(i));
    }
    return builder.build();
  }

  @Override
  public void getLeadership(GetLeadershipRequest request, StreamObserver<GetLeadershipResponse> responseObserver) {
    run(request.getId(), election -> election.getLeadership()
        .thenApply(leadership -> GetLeadershipResponse.newBuilder()
            .setLeadership(toLeadership(leadership))
            .build()), responseObserver);
  }

  @Override
  public void run(RunRequest request, StreamObserver<RunResponse> responseObserver) {
    run(request.getId(), election -> election.run(request.getCandidate())
        .thenApply(leadership -> RunResponse.newBuilder()
            .setLeadership(toLeadership(leadership))
            .build()), responseObserver);
  }

  @Override
  public void withdraw(WithdrawRequest request, StreamObserver<WithdrawResponse> responseObserver) {
    run(request.getId(), election -> election.withdraw(request.getCandidate())
        .thenApply(v -> WithdrawResponse.newBuilder().build()), responseObserver);
  }

  @Override
  public void anoint(AnointRequest request, StreamObserver<AnointResponse> responseObserver) {
    run(request.getId(), election -> election.anoint(request.getCandidate())
        .thenApply(succeeded -> AnointResponse.newBuilder().setSucceeded(succeeded).build()), responseObserver);
  }

  @Override
  public void promote(PromoteRequest request, StreamObserver<PromoteResponse> responseObserver) {
    run(request.getId(), election -> election.promote(request.getCandidate())
        .thenApply(succeeded -> PromoteResponse.newBuilder().setSucceeded(succeeded).build()), responseObserver);
  }

  @Override
  public void evict(EvictRequest request, StreamObserver<EvictResponse> responseObserver) {
    run(request.getId(), election -> election.evict(request.getCandidate())
        .thenApply(v -> EvictResponse.newBuilder().build()), responseObserver);
  }

  @Override
  public StreamObserver<ElectionId> listen(StreamObserver<ElectionEvent> responseObserver) {
    Map<ElectionId, LeadershipEventListener<String>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<ElectionId>() {
      @Override
      public void onNext(ElectionId id) {
        LeadershipEventListener<String> listener = event -> {
          responseObserver.onNext(ElectionEvent.newBuilder()
              .setId(id)
              .setLeadership(toLeadership(event.newLeadership()))
              .build());
        };
        listeners.put(id, listener);
        getElection(id).thenAccept(map -> map.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> getElection(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> getElection(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
