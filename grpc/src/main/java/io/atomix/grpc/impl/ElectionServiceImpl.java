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

import com.google.protobuf.Empty;
import io.atomix.core.Atomix;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.grpc.election.CandidateId;
import io.atomix.grpc.election.ElectionEvent;
import io.atomix.grpc.election.ElectionId;
import io.atomix.grpc.election.ElectionServiceGrpc;
import io.atomix.grpc.election.LeaderId;
import io.atomix.grpc.election.Leadership;
import io.atomix.grpc.election.Succeeded;
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
      return MultiPrimaryProtocol.builder(id.getRaft().getGroup())
          .build();
    } else if (id.hasLog()) {
      return DistributedLogProtocol.builder(id.getRaft().getGroup())
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

  private LeaderId toLeader(Leader<String> leader) {
    return LeaderId.newBuilder()
        .setId(leader.id())
        .setTerm(leader.term())
        .build();
  }

  private CandidateId toCandidate(String candidate) {
    return CandidateId.newBuilder()
        .setId(candidate)
        .build();
  }

  private Leadership toLeadership(io.atomix.core.election.Leadership<String> leadership) {
    Leadership.Builder builder = Leadership.newBuilder();
    builder.setLeader(toLeader(leadership.leader()));
    for (int i = 0; i < leadership.candidates().size(); i++) {
      builder.setCandidates(i, toCandidate(leadership.candidates().get(i)));
    }
    return builder.build();
  }

  private Succeeded toSucceeded(boolean succeeded) {
    return Succeeded.newBuilder().setSucceeded(succeeded).build();
  }

  private Empty toEmpty(Object value) {
    return Empty.newBuilder().build();
  }

  @Override
  public void getLeadership(ElectionId request, StreamObserver<Leadership> responseObserver) {
    run(request, election -> election.getLeadership().thenApply(this::toLeadership), responseObserver);
  }

  @Override
  public void run(CandidateId request, StreamObserver<Leadership> responseObserver) {
    run(request.getElectionId(), election -> election.run(request.getId()).thenApply(this::toLeadership), responseObserver);
  }

  @Override
  public void withdraw(CandidateId request, StreamObserver<Empty> responseObserver) {
    run(request.getElectionId(), election -> election.withdraw(request.getId()).thenApply(this::toEmpty), responseObserver);
  }

  @Override
  public void anoint(CandidateId request, StreamObserver<Succeeded> responseObserver) {
    run(request.getElectionId(), election -> election.anoint(request.getId()).thenApply(this::toSucceeded), responseObserver);
  }

  @Override
  public void promote(CandidateId request, StreamObserver<Succeeded> responseObserver) {
    run(request.getElectionId(), election -> election.promote(request.getId()).thenApply(this::toSucceeded), responseObserver);
  }

  @Override
  public void evict(CandidateId request, StreamObserver<Empty> responseObserver) {
    run(request.getElectionId(), election -> election.evict(request.getId()).thenApply(this::toEmpty), responseObserver);
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
