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
package io.atomix.node.primitive.election;

import java.time.Duration;
import java.util.stream.Collectors;

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
import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.StreamHeader;
import io.atomix.node.primitive.util.PrimitiveFactory;
import io.atomix.node.primitive.util.RequestExecutor;
import io.atomix.node.service.client.ClientFactory;
import io.atomix.node.service.protocol.CloseSessionRequest;
import io.atomix.node.service.protocol.OpenSessionRequest;
import io.atomix.node.service.protocol.SessionCommandContext;
import io.atomix.node.service.protocol.SessionQueryContext;
import io.atomix.node.service.protocol.SessionStreamContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * gRPC leader election service implementation.
 */
public class LeaderElectionService extends LeaderElectionServiceGrpc.LeaderElectionServiceImplBase {
    private final RequestExecutor<LeaderElectionProxy> executor;

    public LeaderElectionService(ClientFactory factory) {
        this.executor = new RequestExecutor<>(new PrimitiveFactory<>(
            LeaderElectionStateMachine.TYPE,
            id -> new LeaderElectionProxy(factory.newSessionClient(id))));
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), CreateResponse::getDefaultInstance, responseObserver,
            election -> election.openSession(OpenSessionRequest.newBuilder()
                .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                    .plusNanos(request.getTimeout().getNanos())
                    .toMillis())
                .build())
                .thenApply(response -> CreateResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(response.getSessionId())
                        .build())
                    .build()));
    }

    @Override
    public void keepAlive(KeepAliveRequest request, StreamObserver<KeepAliveResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), KeepAliveResponse::getDefaultInstance, responseObserver,
            election -> election.keepAlive(io.atomix.node.service.protocol.KeepAliveRequest.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setCommandSequence(request.getHeader().getSequenceNumber())
                .build())
                .thenApply(response -> KeepAliveResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(request.getHeader().getSessionId())
                        .build())
                    .build()));
    }

    @Override
    public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), CloseResponse::getDefaultInstance, responseObserver,
            election -> election.closeSession(CloseSessionRequest.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .build()).thenApply(response -> CloseResponse.newBuilder().build()));
    }

    @Override
    public void enter(EnterRequest request, StreamObserver<EnterResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), EnterResponse::getDefaultInstance, responseObserver,
            election -> election.enter(
                SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.election.EnterRequest.newBuilder()
                    .setId(request.getCandidateId())
                    .build())
                .thenApply(response -> EnterResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(request.getHeader().getSessionId())
                        .setIndex(response.getLeft().getIndex())
                        .setSequenceNumber(response.getLeft().getSequence())
                        .addAllStreams(response.getLeft().getStreamsList().stream()
                            .map(stream -> StreamHeader.newBuilder()
                                .setStreamId(stream.getStreamId())
                                .setIndex(stream.getIndex())
                                .setLastItemNumber(stream.getSequence())
                                .build())
                            .collect(Collectors.toList()))
                        .build())
                    .setTerm(response.getRight().getTerm())
                    .setTimestamp(response.getRight().getTimestamp())
                    .setLeader(response.getRight().getLeader())
                    .addAllCandidates(response.getRight().getCandidatesList())
                    .build()));
    }

    @Override
    public void withdraw(WithdrawRequest request, StreamObserver<WithdrawResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), WithdrawResponse::getDefaultInstance, responseObserver,
            election -> election.withdraw(
                SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.election.WithdrawRequest.newBuilder()
                    .setId(request.getCandidateId())
                    .build())
                .thenApply(response -> WithdrawResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(request.getHeader().getSessionId())
                        .setIndex(response.getLeft().getIndex())
                        .setSequenceNumber(response.getLeft().getSequence())
                        .addAllStreams(response.getLeft().getStreamsList().stream()
                            .map(stream -> StreamHeader.newBuilder()
                                .setStreamId(stream.getStreamId())
                                .setIndex(stream.getIndex())
                                .setLastItemNumber(stream.getSequence())
                                .build())
                            .collect(Collectors.toList()))
                        .build())
                    .setSucceeded(response.getRight().getSucceeded())
                    .build()));
    }

    @Override
    public void anoint(AnointRequest request, StreamObserver<AnointResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), AnointResponse::getDefaultInstance, responseObserver,
            election -> election.anoint(
                SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.election.AnointRequest.newBuilder()
                    .setId(request.getCandidateId())
                    .build())
                .thenApply(response -> AnointResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(request.getHeader().getSessionId())
                        .setIndex(response.getLeft().getIndex())
                        .setSequenceNumber(response.getLeft().getSequence())
                        .addAllStreams(response.getLeft().getStreamsList().stream()
                            .map(stream -> StreamHeader.newBuilder()
                                .setStreamId(stream.getStreamId())
                                .setIndex(stream.getIndex())
                                .setLastItemNumber(stream.getSequence())
                                .build())
                            .collect(Collectors.toList()))
                        .build())
                    .setSucceeded(response.getRight().getSucceeded())
                    .build()));
    }

    @Override
    public void promote(PromoteRequest request, StreamObserver<PromoteResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), PromoteResponse::getDefaultInstance, responseObserver,
            election -> election.promote(
                SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.election.PromoteRequest.newBuilder()
                    .setId(request.getCandidateId())
                    .build())
                .thenApply(response -> PromoteResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(request.getHeader().getSessionId())
                        .setIndex(response.getLeft().getIndex())
                        .setSequenceNumber(response.getLeft().getSequence())
                        .addAllStreams(response.getLeft().getStreamsList().stream()
                            .map(stream -> StreamHeader.newBuilder()
                                .setStreamId(stream.getStreamId())
                                .setIndex(stream.getIndex())
                                .setLastItemNumber(stream.getSequence())
                                .build())
                            .collect(Collectors.toList()))
                        .build())
                    .setSucceeded(response.getRight().getSucceeded())
                    .build()));
    }

    @Override
    public void evict(EvictRequest request, StreamObserver<EvictResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), EvictResponse::getDefaultInstance, responseObserver,
            election -> election.evict(
                SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.election.EvictRequest.newBuilder()
                    .setId(request.getCandidateId())
                    .build())
                .thenApply(response -> EvictResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(request.getHeader().getSessionId())
                        .setIndex(response.getLeft().getIndex())
                        .setSequenceNumber(response.getLeft().getSequence())
                        .addAllStreams(response.getLeft().getStreamsList().stream()
                            .map(stream -> StreamHeader.newBuilder()
                                .setStreamId(stream.getStreamId())
                                .setIndex(stream.getIndex())
                                .setLastItemNumber(stream.getSequence())
                                .build())
                            .collect(Collectors.toList()))
                        .build())
                    .setSucceeded(response.getRight().getSucceeded())
                    .build()));
    }

    @Override
    public void getLeadership(GetLeadershipRequest request, StreamObserver<GetLeadershipResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), GetLeadershipResponse::getDefaultInstance, responseObserver,
            election -> election.getLeadership(
                SessionQueryContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setLastIndex(request.getHeader().getIndex())
                    .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.election.GetLeadershipRequest.newBuilder().build())
                .thenApply(response -> GetLeadershipResponse.newBuilder()
                    .setHeader(ResponseHeader.newBuilder()
                        .setSessionId(request.getHeader().getSessionId())
                        .setIndex(response.getLeft().getIndex())
                        .setSequenceNumber(response.getLeft().getSequence())
                        .addAllStreams(response.getLeft().getStreamsList().stream()
                            .map(stream -> StreamHeader.newBuilder()
                                .setStreamId(stream.getStreamId())
                                .setIndex(stream.getIndex())
                                .setLastItemNumber(stream.getSequence())
                                .build())
                            .collect(Collectors.toList()))
                        .build())
                    .setTerm(response.getRight().getTerm())
                    .setTimestamp(response.getRight().getTimestamp())
                    .setLeader(response.getRight().getLeader())
                    .addAllCandidates(response.getRight().getCandidatesList())
                    .build()));
    }

    @Override
    public void events(EventRequest request, StreamObserver<EventResponse> responseObserver) {
        executor.<Pair<SessionStreamContext, ListenResponse>, EventResponse>execute(request.getHeader().getName(), EventResponse::getDefaultInstance, responseObserver,
            (election, handler) -> election.listen(SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                ListenRequest.newBuilder().build(), handler),
            response -> EventResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addStreams(StreamHeader.newBuilder()
                        .setStreamId(response.getLeft().getStreamId())
                        .setIndex(response.getLeft().getIndex())
                        .setLastItemNumber(response.getLeft().getSequence())
                        .build())
                    .build())
                .setType(EventResponse.Type.valueOf(response.getRight().getType().name()))
                .setTerm(response.getRight().getTerm())
                .setTimestamp(response.getRight().getTimestamp())
                .setLeader(response.getRight().getLeader())
                .addAllCandidates(response.getRight().getCandidatesList())
                .build());
    }

}
