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
package io.atomix.server.service.set;

import java.time.Duration;
import java.util.stream.Collectors;

import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.headers.SessionStreamHeader;
import io.atomix.api.set.AddRequest;
import io.atomix.api.set.AddResponse;
import io.atomix.api.set.ClearRequest;
import io.atomix.api.set.ClearResponse;
import io.atomix.api.set.CloseRequest;
import io.atomix.api.set.CloseResponse;
import io.atomix.api.set.ContainsRequest;
import io.atomix.api.set.ContainsResponse;
import io.atomix.api.set.CreateRequest;
import io.atomix.api.set.CreateResponse;
import io.atomix.api.set.EventRequest;
import io.atomix.api.set.EventResponse;
import io.atomix.api.set.IterateRequest;
import io.atomix.api.set.IterateResponse;
import io.atomix.api.set.KeepAliveRequest;
import io.atomix.api.set.KeepAliveResponse;
import io.atomix.api.set.RemoveRequest;
import io.atomix.api.set.RemoveResponse;
import io.atomix.api.set.SetServiceGrpc;
import io.atomix.api.set.SizeRequest;
import io.atomix.api.set.SizeResponse;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.RequestExecutor;
import io.atomix.server.protocol.ServiceProtocol;
import io.atomix.server.protocol.impl.DefaultSessionClient;
import io.atomix.service.protocol.CloseSessionRequest;
import io.atomix.service.protocol.OpenSessionRequest;
import io.atomix.service.protocol.SessionCommandContext;
import io.atomix.service.protocol.SessionQueryContext;
import io.atomix.service.protocol.SessionStreamContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Set service implementation.
 */
public class SetServiceImpl extends SetServiceGrpc.SetServiceImplBase {
  private final RequestExecutor<SetProxy> executor;

  public SetServiceImpl(ServiceProtocol protocol) {
    this.executor = new RequestExecutor<>(new PrimitiveFactory<>(
        protocol.getServiceClient(),
        SetService.TYPE,
        (id, client) -> new SetProxy(new DefaultSessionClient(id, client))));
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    executor.execute(request.getSetId(), CreateResponse::getDefaultInstance, responseObserver,
        set -> set.openSession(OpenSessionRequest.newBuilder()
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> CreateResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(response.getSessionId())
                    .build())
                .build()));
  }

  @Override
  public void keepAlive(KeepAliveRequest request, StreamObserver<KeepAliveResponse> responseObserver) {
    executor.execute(request.getSetId(), KeepAliveResponse::getDefaultInstance, responseObserver,
        set -> set.keepAlive(io.atomix.service.protocol.KeepAliveRequest.newBuilder()
            .setSessionId(request.getHeader().getSessionId())
            .setCommandSequence(request.getHeader().getLastSequenceNumber())
            .build())
            .thenApply(response -> KeepAliveResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .build())
                .build()));
  }

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    executor.execute(request.getSetId(), CloseResponse::getDefaultInstance, responseObserver,
        set -> set.closeSession(CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeader().getSessionId())
            .build())
            .thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void add(AddRequest request, StreamObserver<AddResponse> responseObserver) {
    executor.execute(request.getSetId(), AddResponse::getDefaultInstance, responseObserver,
        set -> set.add(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.set.AddRequest.newBuilder()
                .addAllValues(request.getValuesList())
                .build())
            .thenApply(response -> AddResponse.newBuilder()
                .setAdded(response.getRight().getAdded())
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
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
    executor.execute(request.getSetId(), RemoveResponse::getDefaultInstance, responseObserver,
        set -> set.remove(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.set.RemoveRequest.newBuilder()
                .addAllValues(request.getValuesList())
                .build())
            .thenApply(response -> RemoveResponse.newBuilder()
                .setRemoved(response.getRight().getRemoved())
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
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
    executor.execute(request.getSetId(), ContainsResponse::getDefaultInstance, responseObserver,
        set -> set.contains(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getLastIndex())
                .setLastSequenceNumber(request.getHeader().getLastSequenceNumber())
                .build(),
            io.atomix.server.service.set.ContainsRequest.newBuilder()
                .addAllValues(request.getValuesList())
                .build())
            .thenApply(response -> ContainsResponse.newBuilder()
                .setContains(response.getRight().getContains())
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                .build()));
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    executor.execute(request.getSetId(), SizeResponse::getDefaultInstance, responseObserver,
        set -> set.size(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getLastIndex())
                .setLastSequenceNumber(request.getHeader().getLastSequenceNumber())
                .build(),
            io.atomix.server.service.set.SizeRequest.newBuilder().build())
            .thenApply(response -> SizeResponse.newBuilder()
                .setSize(response.getRight().getSize())
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
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
    executor.execute(request.getSetId(), ClearResponse::getDefaultInstance, responseObserver,
        set -> set.clear(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.set.ClearRequest.newBuilder().build())
            .thenApply(response -> ClearResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(request.getHeader().getSessionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addAllStreams(response.getLeft().getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
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
    executor.<Pair<SessionStreamContext, ListenResponse>, EventResponse>execute(request.getSetId(), EventResponse::getDefaultInstance, responseObserver,
        (set, handler) -> set.listen(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            ListenRequest.newBuilder().build(), handler),
        response -> EventResponse.newBuilder()
            .setHeader(SessionResponseHeader.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setIndex(response.getLeft().getIndex())
                .setSequenceNumber(response.getLeft().getSequence())
                .addStreams(SessionStreamHeader.newBuilder()
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
    executor.<Pair<SessionStreamContext, io.atomix.server.service.set.IterateResponse>, IterateResponse>execute(request.getSetId(), IterateResponse::getDefaultInstance, responseObserver,
        (set, handler) -> set.iterate(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getLastIndex())
                .setLastSequenceNumber(request.getHeader().getLastSequenceNumber())
                .build(),
            io.atomix.server.service.set.IterateRequest.newBuilder().build(), handler),
        response -> IterateResponse.newBuilder()
            .setHeader(SessionResponseHeader.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setIndex(response.getLeft().getIndex())
                .setSequenceNumber(response.getLeft().getSequence())
                .addStreams(SessionStreamHeader.newBuilder()
                    .setStreamId(response.getLeft().getStreamId())
                    .setIndex(response.getLeft().getIndex())
                    .setLastItemNumber(response.getLeft().getSequence())
                    .build())
                .build())
            .setValue(response.getRight().getValue())
            .build());
  }
}
