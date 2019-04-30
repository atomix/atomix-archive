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
import java.util.stream.Collectors;

import io.atomix.core.Atomix;
import io.atomix.core.value.impl.ListenResponse;
import io.atomix.core.value.impl.ValueProxy;
import io.atomix.core.value.impl.ValueService;
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
import io.atomix.grpc.value.CheckAndSetRequest;
import io.atomix.grpc.value.CheckAndSetResponse;
import io.atomix.grpc.value.CloseRequest;
import io.atomix.grpc.value.CloseResponse;
import io.atomix.grpc.value.CreateRequest;
import io.atomix.grpc.value.CreateResponse;
import io.atomix.grpc.value.EventRequest;
import io.atomix.grpc.value.EventResponse;
import io.atomix.grpc.value.GetRequest;
import io.atomix.grpc.value.GetResponse;
import io.atomix.grpc.value.KeepAliveRequest;
import io.atomix.grpc.value.KeepAliveResponse;
import io.atomix.grpc.value.SetRequest;
import io.atomix.grpc.value.SetResponse;
import io.atomix.grpc.value.ValueId;
import io.atomix.grpc.value.ValueServiceGrpc;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Counter service implementation.
 */
public class ValueServiceImpl extends ValueServiceGrpc.ValueServiceImplBase {
  private final PrimitiveFactory<ValueProxy, ValueId> primitiveFactory;
  private final RequestExecutor<ValueProxy, ValueId, SessionHeader, CreateRequest, CreateResponse> create;
  private final RequestExecutor<ValueProxy, ValueId, SessionHeader, KeepAliveRequest, KeepAliveResponse> keepAlive;
  private final RequestExecutor<ValueProxy, ValueId, SessionHeader, CloseRequest, CloseResponse> close;
  private final RequestExecutor<ValueProxy, ValueId, SessionQueryHeader, GetRequest, GetResponse> get;
  private final RequestExecutor<ValueProxy, ValueId, SessionCommandHeader, SetRequest, SetResponse> set;
  private final RequestExecutor<ValueProxy, ValueId, SessionCommandHeader, CheckAndSetRequest, CheckAndSetResponse> checkAndSet;
  private final RequestExecutor<ValueProxy, ValueId, SessionCommandHeader, EventRequest, EventResponse> events;

  public ValueServiceImpl(Atomix atomix) {
    this.primitiveFactory = new PrimitiveFactory<>(atomix, ValueService.TYPE, ValueProxy::new, VALUE_ID_DESCRIPTOR);
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, CloseResponse::getDefaultInstance);
    this.get = new RequestExecutor<>(primitiveFactory, GET_DESCRIPTOR, GetResponse::getDefaultInstance);
    this.set = new RequestExecutor<>(primitiveFactory, SET_DESCRIPTOR, SetResponse::getDefaultInstance);
    this.checkAndSet = new RequestExecutor<>(primitiveFactory, CHECK_AND_SET_DESCRIPTOR, CheckAndSetResponse::getDefaultInstance);
    this.events = new RequestExecutor<>(primitiveFactory, EVENT_DESCRIPTOR, EventResponse::getDefaultInstance);
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, sessionId, value) -> value.openSession(OpenSessionRequest.newBuilder()
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
        (partitionId, header, value) -> value.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
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
        (partitionId, header, value) -> value.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeaders().getSessionId())
            .build()).thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    set.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, value) -> value.set(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.value.impl.SetRequest.newBuilder()
                .setValue(request.getValue())
                .build())
            .thenApply(response -> SetResponse.newBuilder()
                .setHeaders(SessionResponseHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
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
                    .build())
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    get.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, value) -> value.get(
            SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.value.impl.GetRequest.newBuilder().build())
            .thenApply(response -> GetResponse.newBuilder()
                .setHeaders(SessionResponseHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
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
                    .build())
                .setValue(response.getRight().getValue())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void checkAndSet(CheckAndSetRequest request, StreamObserver<CheckAndSetResponse> responseObserver) {
    checkAndSet.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, value) -> value.checkAndSet(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.value.impl.CheckAndSetRequest.newBuilder()
                .setCheck(request.getCheck())
                .setUpdate(request.getUpdate())
                .setVersion(request.getVersion())
                .build())
            .thenApply(response -> CheckAndSetResponse.newBuilder()
                .setHeaders(SessionResponseHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
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
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void event(EventRequest request, StreamObserver<EventResponse> responseObserver) {
    events.<Pair<SessionStreamContext, ListenResponse>>executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, handler, value) -> value.listen(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.value.impl.ListenRequest.newBuilder().build(),
            handler),
        (partitionId, header, event) -> EventResponse.newBuilder()
            .setHeaders(SessionResponseHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addHeaders(SessionResponseHeader.newBuilder()
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(event.getLeft().getIndex())
                    .setSequenceNumber(event.getLeft().getSequence())
                    .addStreams(SessionStreamHeader.newBuilder()
                        .setStreamId(event.getLeft().getStreamId())
                        .setIndex(event.getLeft().getIndex())
                        .setLastItemNumber(event.getLeft().getSequence())
                        .build())
                    .build())
                .build())
            .setType(EventResponse.Type.valueOf(event.getRight().getType().name()))
            .setPreviousValue(event.getRight().getPreviousValue())
            .setPreviousVersion(event.getRight().getPreviousVersion())
            .setNewValue(event.getRight().getNewValue())
            .setNewVersion(event.getRight().getNewVersion())
            .build());
  }

  private static final PrimitiveFactory.PrimitiveIdDescriptor<ValueId> VALUE_ID_DESCRIPTOR = new PrimitiveFactory.PrimitiveIdDescriptor<ValueId>() {
    @Override
    public String getName(ValueId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(ValueId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(ValueId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(ValueId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(ValueId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(ValueId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(ValueId id) {
      return id.getLog();
    }
  };

  private static final RequestExecutor.RequestDescriptor<CreateRequest, ValueId, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CreateRequest::getId, r -> SessionHeaders.getDefaultInstance());

  private static final RequestExecutor.RequestDescriptor<KeepAliveRequest, ValueId, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(KeepAliveRequest::getId, KeepAliveRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<CloseRequest, ValueId, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CloseRequest::getId, CloseRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<GetRequest, ValueId, SessionQueryHeader> GET_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(GetRequest::getId, GetRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<SetRequest, ValueId, SessionCommandHeader> SET_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(SetRequest::getId, SetRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<CheckAndSetRequest, ValueId, SessionCommandHeader> CHECK_AND_SET_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(CheckAndSetRequest::getId, CheckAndSetRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<EventRequest, ValueId, SessionCommandHeader> EVENT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(EventRequest::getId, EventRequest::getHeaders);
}
