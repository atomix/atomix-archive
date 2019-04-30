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

import java.time.Duration;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.atomix.core.Atomix;
import io.atomix.core.set.impl.ListenResponse;
import io.atomix.core.set.impl.SetProxy;
import io.atomix.core.set.impl.SetService;
import io.atomix.grpc.headers.SessionCommandHeader;
import io.atomix.grpc.headers.SessionHeader;
import io.atomix.grpc.headers.SessionHeaders;
import io.atomix.grpc.headers.SessionQueryHeader;
import io.atomix.grpc.headers.SessionResponseHeader;
import io.atomix.grpc.headers.SessionResponseHeaders;
import io.atomix.grpc.headers.SessionStreamHeader;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.atomix.grpc.protocol.MultiPrimaryProtocol;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.grpc.set.AddRequest;
import io.atomix.grpc.set.AddResponse;
import io.atomix.grpc.set.ClearRequest;
import io.atomix.grpc.set.ClearResponse;
import io.atomix.grpc.set.CloseRequest;
import io.atomix.grpc.set.CloseResponse;
import io.atomix.grpc.set.ContainsRequest;
import io.atomix.grpc.set.ContainsResponse;
import io.atomix.grpc.set.CreateRequest;
import io.atomix.grpc.set.CreateResponse;
import io.atomix.grpc.set.EventRequest;
import io.atomix.grpc.set.EventResponse;
import io.atomix.grpc.set.IterateRequest;
import io.atomix.grpc.set.IterateResponse;
import io.atomix.grpc.set.KeepAliveRequest;
import io.atomix.grpc.set.KeepAliveResponse;
import io.atomix.grpc.set.RemoveRequest;
import io.atomix.grpc.set.RemoveResponse;
import io.atomix.grpc.set.SetId;
import io.atomix.grpc.set.SetServiceGrpc;
import io.atomix.grpc.set.SizeRequest;
import io.atomix.grpc.set.SizeResponse;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Set service implementation.
 */
public class SetServiceImpl extends SetServiceGrpc.SetServiceImplBase {
  private final PrimitiveFactory<SetProxy, SetId> primitiveFactory;
  private final RequestExecutor<SetProxy, SetId, SessionHeader, CreateRequest, CreateResponse> create;
  private final RequestExecutor<SetProxy, SetId, SessionHeader, KeepAliveRequest, KeepAliveResponse> keepAlive;
  private final RequestExecutor<SetProxy, SetId, SessionHeader, CloseRequest, CloseResponse> close;
  private final RequestExecutor<SetProxy, SetId, SessionCommandHeader, AddRequest, AddResponse> add;
  private final RequestExecutor<SetProxy, SetId, SessionCommandHeader, RemoveRequest, RemoveResponse> remove;
  private final RequestExecutor<SetProxy, SetId, SessionQueryHeader, ContainsRequest, ContainsResponse> contains;
  private final RequestExecutor<SetProxy, SetId, SessionQueryHeader, SizeRequest, SizeResponse> size;
  private final RequestExecutor<SetProxy, SetId, SessionCommandHeader, ClearRequest, ClearResponse> clear;
  private final RequestExecutor<SetProxy, SetId, SessionCommandHeader, EventRequest, EventResponse> listen;
  private final RequestExecutor<SetProxy, SetId, SessionQueryHeader, IterateRequest, IterateResponse> iterate;

  public SetServiceImpl(Atomix atomix) {
    this.primitiveFactory = new PrimitiveFactory<>(atomix, SetService.TYPE, SetProxy::new, SET_ID_DESCRIPTOR);
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, CloseResponse::getDefaultInstance);
    this.add = new RequestExecutor<>(primitiveFactory, ADD_DESCRIPTOR, AddResponse::getDefaultInstance);
    this.remove = new RequestExecutor<>(primitiveFactory, REMOVE_DESCRIPTOR, RemoveResponse::getDefaultInstance);
    this.contains = new RequestExecutor<>(primitiveFactory, CONTAINS_DESCRIPTOR, ContainsResponse::getDefaultInstance);
    this.size = new RequestExecutor<>(primitiveFactory, SIZE_DESCRIPTOR, SizeResponse::getDefaultInstance);
    this.clear = new RequestExecutor<>(primitiveFactory, CLEAR_DESCRIPTOR, ClearResponse::getDefaultInstance);
    this.listen = new RequestExecutor<>(primitiveFactory, EVENT_DESCRIPTOR, EventResponse::getDefaultInstance);
    this.iterate = new RequestExecutor<>(primitiveFactory, ITERATE_DESCRIPTOR, IterateResponse::getDefaultInstance);
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, sessionId, set) -> set.openSession(OpenSessionRequest.newBuilder()
            .setSessionId(sessionId)
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> CreateResponse.newBuilder()
                .setHeaders(SessionHeaders.newBuilder()
                    .setSessionId(sessionId)
                    .build())
                .build()));
  }

  @Override
  public void keepAlive(KeepAliveRequest request, StreamObserver<KeepAliveResponse> responseObserver) {
    keepAlive.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, set) -> set.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(request.getHeaders().getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> KeepAliveResponse.newBuilder()
                .setHeaders(SessionHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
                        .build())
                    .build())
                .build()));
  }

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    close.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, set) -> set.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeaders().getSessionId())
            .build()).thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void add(AddRequest request, StreamObserver<AddResponse> responseObserver) {
    add.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.add(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.set.impl.AddRequest.newBuilder()
                .addAllValues(request.getValuesList())
                .build()),
        responses -> AddResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addAllHeaders(responses.map(Pair::getLeft).map(context -> SessionResponseHeader.newBuilder()
                    .setIndex(context.getIndex())
                    .setSequenceNumber(context.getSequence())
                    .addAllStreams(context.getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                    .collect(Collectors.toList()))
                .build())
            .setAdded(responses.map(Pair::getRight)
                .map(io.atomix.core.set.impl.AddResponse::getAdded)
                .reduce(Boolean::logicalOr)
                .orElse(false))
            .build());
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    remove.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.remove(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.set.impl.RemoveRequest.newBuilder()
                .addAllValues(request.getValuesList())
                .build()),
        responses -> RemoveResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addAllHeaders(responses.map(Pair::getLeft).map(context -> SessionResponseHeader.newBuilder()
                    .setIndex(context.getIndex())
                    .setSequenceNumber(context.getSequence())
                    .addAllStreams(context.getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                    .collect(Collectors.toList()))
                .build())
            .setRemoved(responses.map(Pair::getRight)
                .map(io.atomix.core.set.impl.RemoveResponse::getRemoved)
                .reduce(Boolean::logicalOr)
                .orElse(false))
            .build());
  }

  @Override
  public void contains(ContainsRequest request, StreamObserver<ContainsResponse> responseObserver) {
    contains.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.contains(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.set.impl.ContainsRequest.newBuilder()
                .addAllValues(request.getValuesList())
                .build()),
        responses -> ContainsResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addAllHeaders(responses.map(Pair::getLeft).map(context -> SessionResponseHeader.newBuilder()
                    .setIndex(context.getIndex())
                    .setSequenceNumber(context.getSequence())
                    .addAllStreams(context.getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                    .collect(Collectors.toList()))
                .build())
            .setContains(responses.map(Pair::getRight)
                .map(io.atomix.core.set.impl.ContainsResponse::getContains)
                .filter(Predicate.isEqual(true))
                .findFirst()
                .orElse(false))
            .build());
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    size.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.size(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.set.impl.SizeRequest.newBuilder().build()),
        responses -> SizeResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addAllHeaders(responses.map(Pair::getLeft).map(context -> SessionResponseHeader.newBuilder()
                    .setIndex(context.getIndex())
                    .setSequenceNumber(context.getSequence())
                    .addAllStreams(context.getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                    .collect(Collectors.toList()))
                .build())
            .setSize(responses.map(r -> r.getRight().getSize()).reduce(Math::addExact).orElse(0))
            .build());
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    clear.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.clear(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.set.impl.ClearRequest.newBuilder().build()),
        responses -> ClearResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addAllHeaders(responses.map(Pair::getLeft).map(context -> SessionResponseHeader.newBuilder()
                    .setIndex(context.getIndex())
                    .setSequenceNumber(context.getSequence())
                    .addAllStreams(context.getStreamsList().stream()
                        .map(stream -> SessionStreamHeader.newBuilder()
                            .setStreamId(stream.getStreamId())
                            .setIndex(stream.getIndex())
                            .setLastItemNumber(stream.getSequence())
                            .build())
                        .collect(Collectors.toList()))
                    .build())
                    .collect(Collectors.toList()))
                .build())
            .build());
  }

  @Override
  public void listen(EventRequest request, StreamObserver<EventResponse> responseObserver) {
    listen.<Pair<SessionStreamContext, ListenResponse>>executeAll(request, responseObserver,
        (header, handler, set) -> set.listen(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.set.impl.ListenRequest.newBuilder().build(), handler),
        (header, response) -> EventResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addHeaders(SessionResponseHeader.newBuilder()
                    .setPartitionId(header.getPartitionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addStreams(SessionStreamHeader.newBuilder()
                        .setStreamId(response.getLeft().getStreamId())
                        .setIndex(response.getLeft().getIndex())
                        .setLastItemNumber(response.getLeft().getSequence())
                        .build())
                    .build())
                .build())
            .setType(EventResponse.Type.valueOf(response.getRight().getType().name()))
            .setValue(response.getRight().getValue())
            .build());
  }

  @Override
  public void iterate(IterateRequest request, StreamObserver<IterateResponse> responseObserver) {
    iterate.<Pair<SessionStreamContext, io.atomix.core.set.impl.IterateResponse>>executeAll(request, responseObserver,
        (header, handler, set) -> set.iterate(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.set.impl.IterateRequest.newBuilder().build(), handler),
        (header, response) -> IterateResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addHeaders(SessionResponseHeader.newBuilder()
                    .setPartitionId(header.getPartitionId())
                    .setIndex(response.getLeft().getIndex())
                    .setSequenceNumber(response.getLeft().getSequence())
                    .addStreams(SessionStreamHeader.newBuilder()
                        .setStreamId(response.getLeft().getStreamId())
                        .setIndex(response.getLeft().getIndex())
                        .setLastItemNumber(response.getLeft().getSequence())
                        .build())
                    .build())
                .build())
            .setValue(response.getRight().getValue())
            .build());
  }

  private static final PrimitiveFactory.PrimitiveIdDescriptor<SetId> SET_ID_DESCRIPTOR = new PrimitiveFactory.PrimitiveIdDescriptor<SetId>() {
    @Override
    public String getName(SetId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(SetId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(SetId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(SetId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(SetId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(SetId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(SetId id) {
      return id.getLog();
    }
  };

  private static final RequestExecutor.RequestDescriptor<CreateRequest, SetId, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CreateRequest::getId, r -> SessionHeaders.getDefaultInstance());

  private static final RequestExecutor.RequestDescriptor<KeepAliveRequest, SetId, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(KeepAliveRequest::getId, KeepAliveRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<CloseRequest, SetId, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CloseRequest::getId, CloseRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<AddRequest, SetId, SessionCommandHeader> ADD_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(AddRequest::getId, AddRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<RemoveRequest, SetId, SessionCommandHeader> REMOVE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(RemoveRequest::getId, RemoveRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<ContainsRequest, SetId, SessionQueryHeader> CONTAINS_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(ContainsRequest::getId, ContainsRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<SizeRequest, SetId, SessionQueryHeader> SIZE_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(SizeRequest::getId, SizeRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<ClearRequest, SetId, SessionCommandHeader> CLEAR_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(ClearRequest::getId, ClearRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<EventRequest, SetId, SessionCommandHeader> EVENT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(EventRequest::getId, EventRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<IterateRequest, SetId, SessionQueryHeader> ITERATE_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(IterateRequest::getId, IterateRequest::getHeaders);
}
