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
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Strings;
import io.atomix.api.election.AnointRequest;
import io.atomix.api.election.AnointResponse;
import io.atomix.api.election.CloseRequest;
import io.atomix.api.election.CloseResponse;
import io.atomix.api.election.CreateRequest;
import io.atomix.api.election.CreateResponse;
import io.atomix.api.election.ElectionId;
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
import io.atomix.api.protocol.DistributedLogProtocol;
import io.atomix.api.protocol.MultiPrimaryProtocol;
import io.atomix.api.protocol.MultiRaftProtocol;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.channel.ChannelFactory;
import io.atomix.client.election.AsyncLeaderElection;
import io.atomix.client.election.Leader;
import io.atomix.client.election.LeaderElection;
import io.atomix.client.election.Leadership;
import io.atomix.client.election.LeadershipEvent;
import io.atomix.client.election.LeadershipEventListener;
import io.atomix.client.impl.AbstractAsyncPrimitive;
import io.atomix.client.impl.PrimitiveIdDescriptor;
import io.atomix.client.impl.PrimitivePartition;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;
import io.grpc.stub.StreamObserver;

/**
 * Distributed resource providing the {@link AsyncLeaderElection} primitive.
 */
public class DefaultAsyncLeaderElection
    extends AbstractAsyncPrimitive<ElectionId, AsyncLeaderElection<String>>
    implements AsyncLeaderElection<String> {
  private final LeaderElectionServiceGrpc.LeaderElectionServiceStub election;

  public DefaultAsyncLeaderElection(ElectionId id, ChannelFactory channelFactory, PrimitiveManagementService managementService, Partitioner<String> partitioner, Duration timeout) {
    super(id, ELECTION_ID_DESCRIPTOR, managementService, partitioner, timeout);
    this.election = LeaderElectionServiceGrpc.newStub(channelFactory.getChannel());
  }

  @Override
  public CompletableFuture<Leadership<String>> run(String identifier) {
    PrimitivePartition partition = getPartition();
    return this.<EnterResponse>execute(observer -> election.enter(EnterRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setCandidateId(identifier)
        .build(), observer))
        .thenCompose(response -> partition.order(new Leadership<>(!Strings.isNullOrEmpty(response.getLeader())
                ? new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp())
                : null,
                response.getCandidatesList()),
            response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> withdraw(String identifier) {
    PrimitivePartition partition = getPartition();
    return this.<WithdrawResponse>execute(observer -> election.withdraw(WithdrawRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setCandidateId(identifier)
        .build(), observer))
        .thenCompose(response -> partition.order(null, response.getHeader()));
  }

  @Override
  public CompletableFuture<Boolean> anoint(String identifier) {
    PrimitivePartition partition = getPartition();
    return this.<AnointResponse>execute(observer -> election.anoint(AnointRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setCandidateId(identifier)
        .build(), observer))
        .thenCompose(response -> partition.order(response.getSucceeded(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> evict(String identifier) {
    PrimitivePartition partition = getPartition();
    return this.<EvictResponse>execute(observer -> election.evict(EvictRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setCandidateId(identifier)
        .build(), observer))
        .thenCompose(response -> partition.order(null, response.getHeader()));
  }

  @Override
  public CompletableFuture<Boolean> promote(String identifier) {
    PrimitivePartition partition = getPartition();
    return this.<PromoteResponse>execute(observer -> election.promote(PromoteRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setCandidateId(identifier)
        .build(), observer))
        .thenCompose(response -> partition.order(response.getSucceeded(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Leadership<String>> getLeadership() {
    PrimitivePartition partition = getPartition();
    return this.<GetLeadershipResponse>execute(observer -> election.getLeadership(GetLeadershipRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getQueryHeader())
        .build(), observer))
        .thenCompose(response -> partition.order(new Leadership<>(!Strings.isNullOrEmpty(response.getLeader())
                ? new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp())
                : null,
                response.getCandidatesList()),
            response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> addListener(LeadershipEventListener<String> listener) {
    PrimitivePartition partition = getPartition();
    election.events(EventRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .build(), new StreamObserver<EventResponse>() {
      @Override
      public void onNext(EventResponse response) {
        PrimitivePartition partition = getPartition(response.getHeader().getPartitionId());
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
        partition.order(event, response.getHeader()).thenAccept(listener::event);
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(LeadershipEventListener<String> listener) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<AsyncLeaderElection<String>> connect() {
    return this.<CreateResponse>execute(stream -> election.create(CreateRequest.newBuilder()
        .setId(id())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), stream))
        .thenAccept(response -> {
          startKeepAlive(response.getHeader());
        })
        .thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<Void> keepAlive() {
    PrimitivePartition partition = getPartition();
    return this.<KeepAliveResponse>execute(stream -> election.keepAlive(KeepAliveRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getSessionHeader())
        .build(), stream))
        .thenAccept(response -> completeKeepAlive(response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> close() {
    PrimitivePartition partition = getPartition();
    return this.<CloseResponse>execute(stream -> election.close(CloseRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getSessionHeader())
        .build(), stream))
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public LeaderElection<String> sync(Duration operationTimeout) {
    return new BlockingLeaderElection<>(this, operationTimeout.toMillis());
  }

  private static final PrimitiveIdDescriptor<ElectionId> ELECTION_ID_DESCRIPTOR = new PrimitiveIdDescriptor<ElectionId>() {
    @Override
    public String getName(ElectionId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(ElectionId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(ElectionId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(ElectionId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(ElectionId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(ElectionId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(ElectionId id) {
      return id.getLog();
    }
  };
}