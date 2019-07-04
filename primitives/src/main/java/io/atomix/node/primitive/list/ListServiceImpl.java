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
package io.atomix.node.primitive.list;

import java.time.Duration;
import java.util.stream.Collectors;

import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.StreamHeader;
import io.atomix.api.list.AppendRequest;
import io.atomix.api.list.AppendResponse;
import io.atomix.api.list.ClearRequest;
import io.atomix.api.list.ClearResponse;
import io.atomix.api.list.CloseRequest;
import io.atomix.api.list.CloseResponse;
import io.atomix.api.list.ContainsRequest;
import io.atomix.api.list.ContainsResponse;
import io.atomix.api.list.CreateRequest;
import io.atomix.api.list.CreateResponse;
import io.atomix.api.list.EventRequest;
import io.atomix.api.list.EventResponse;
import io.atomix.api.list.InsertRequest;
import io.atomix.api.list.InsertResponse;
import io.atomix.api.list.IterateRequest;
import io.atomix.api.list.IterateResponse;
import io.atomix.api.list.KeepAliveRequest;
import io.atomix.api.list.KeepAliveResponse;
import io.atomix.api.list.ListServiceGrpc;
import io.atomix.api.list.RemoveRequest;
import io.atomix.api.list.RemoveResponse;
import io.atomix.api.list.ResponseStatus;
import io.atomix.api.list.SizeRequest;
import io.atomix.api.list.SizeResponse;
import io.atomix.node.primitive.set.SetService;
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
 * gRPC list service implementation.
 */
public class ListServiceImpl extends ListServiceGrpc.ListServiceImplBase {
    private final RequestExecutor<ListProxy> executor;

    public ListServiceImpl(ClientFactory factory) {
        this.executor = new RequestExecutor<>(new PrimitiveFactory<>(
            SetService.TYPE,
            id -> new ListProxy(factory.newSessionClient(id))));
    }

    @Override
    public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), CreateResponse::getDefaultInstance, responseObserver,
            list -> list.openSession(OpenSessionRequest.newBuilder()
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
            list -> list.keepAlive(io.atomix.node.service.protocol.KeepAliveRequest.newBuilder()
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
            list -> list.closeSession(CloseSessionRequest.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .build())
                .thenApply(response -> CloseResponse.newBuilder().build()));
    }

    @Override
    public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), SizeResponse::getDefaultInstance, responseObserver,
            list -> list.size(SessionQueryContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setLastIndex(request.getHeader().getIndex())
                    .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.list.SizeRequest.newBuilder().build())
                .thenApply(response -> SizeResponse.newBuilder()
                    .setSize(response.getRight().getSize())
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
                    .build()));
    }

    @Override
    public void contains(ContainsRequest request, StreamObserver<ContainsResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), ContainsResponse::getDefaultInstance, responseObserver,
            list -> list.contains(SessionQueryContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setLastIndex(request.getHeader().getIndex())
                    .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.list.ContainsRequest.newBuilder()
                    .setValue(request.getValue())
                    .build())
                .thenApply(response -> ContainsResponse.newBuilder()
                    .setContains(response.getRight().getContains())
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
                    .build()));
    }

    @Override
    public void append(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), AppendResponse::getDefaultInstance, responseObserver,
            list -> list.append(SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.list.AppendRequest.newBuilder()
                    .setValue(request.getValue())
                    .build())
                .thenApply(response -> AppendResponse.newBuilder()
                    .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
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
                    .build()));
    }

    @Override
    public void insert(InsertRequest request, StreamObserver<InsertResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), InsertResponse::getDefaultInstance, responseObserver,
            list -> list.insert(SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.list.InsertRequest.newBuilder()
                    .setIndex(request.getIndex())
                    .setValue(request.getValue())
                    .build())
                .thenApply(response -> InsertResponse.newBuilder()
                    .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
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
                    .build()));
    }

    @Override
    public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), RemoveResponse::getDefaultInstance, responseObserver,
            list -> list.remove(SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.list.RemoveRequest.newBuilder()
                    .setIndex(request.getIndex())
                    .build())
                .thenApply(response -> RemoveResponse.newBuilder()
                    .setValue(response.getRight().getValue())
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
                    .build()));
    }

    @Override
    public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
        executor.execute(request.getHeader().getName(), ClearResponse::getDefaultInstance, responseObserver,
            list -> list.clear(SessionCommandContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.list.ClearRequest.newBuilder().build())
                .thenApply(response -> ClearResponse.newBuilder()
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
                    .build()));
    }

    @Override
    public void listen(EventRequest request, StreamObserver<EventResponse> responseObserver) {
        executor.<Pair<SessionStreamContext, ListenResponse>, EventResponse>execute(request.getHeader().getName(), EventResponse::getDefaultInstance, responseObserver,
            (list, handler) -> list.listen(SessionCommandContext.newBuilder()
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
                .setValue(response.getRight().getValue())
                .build());
    }

    @Override
    public void iterate(IterateRequest request, StreamObserver<IterateResponse> responseObserver) {
        executor.<Pair<SessionStreamContext, io.atomix.node.primitive.list.IterateResponse>, IterateResponse>execute(request.getHeader().getName(), IterateResponse::getDefaultInstance, responseObserver,
            (list, handler) -> list.iterate(SessionQueryContext.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setLastIndex(request.getHeader().getIndex())
                    .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                    .build(),
                io.atomix.node.primitive.list.IterateRequest.newBuilder().build(), handler),
            response -> IterateResponse.newBuilder()
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
                .setValue(response.getRight().getValue())
                .build());
    }
}
