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
package io.atomix.server.value;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.headers.SessionStreamHeader;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.session.impl.DefaultSessionClient;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.RequestExecutor;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Value service implementation.
 */
public class ValueServiceImpl extends io.atomix.api.value.ValueServiceGrpc.ValueServiceImplBase {
  private final PrimitiveFactory<ValueProxy> primitiveFactory;
  private final RequestExecutor<ValueProxy, SessionHeader, io.atomix.api.value.CreateRequest, io.atomix.api.value.CreateResponse> create;
  private final RequestExecutor<ValueProxy, SessionHeader, io.atomix.api.value.KeepAliveRequest, io.atomix.api.value.KeepAliveResponse> keepAlive;
  private final RequestExecutor<ValueProxy, SessionHeader, io.atomix.api.value.CloseRequest, io.atomix.api.value.CloseResponse> close;
  private final RequestExecutor<ValueProxy, SessionQueryHeader, io.atomix.api.value.GetRequest, io.atomix.api.value.GetResponse> get;
  private final RequestExecutor<ValueProxy, SessionCommandHeader, io.atomix.api.value.SetRequest, io.atomix.api.value.SetResponse> set;
  private final RequestExecutor<ValueProxy, SessionCommandHeader, io.atomix.api.value.CheckAndSetRequest, io.atomix.api.value.CheckAndSetResponse> checkAndSet;
  private final RequestExecutor<ValueProxy, SessionCommandHeader, io.atomix.api.value.EventRequest, io.atomix.api.value.EventResponse> events;

  public ValueServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(
        partitionService,
        ValueService.TYPE,
        (id, client) -> new ValueProxy(new DefaultSessionClient(id, client)));
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, io.atomix.api.value.CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, io.atomix.api.value.KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, io.atomix.api.value.CloseResponse::getDefaultInstance);
    this.get = new RequestExecutor<>(primitiveFactory, GET_DESCRIPTOR, io.atomix.api.value.GetResponse::getDefaultInstance);
    this.set = new RequestExecutor<>(primitiveFactory, SET_DESCRIPTOR, io.atomix.api.value.SetResponse::getDefaultInstance);
    this.checkAndSet = new RequestExecutor<>(primitiveFactory, CHECK_AND_SET_DESCRIPTOR, io.atomix.api.value.CheckAndSetResponse::getDefaultInstance);
    this.events = new RequestExecutor<>(primitiveFactory, EVENT_DESCRIPTOR, io.atomix.api.value.EventResponse::getDefaultInstance);
  }

  @Override
  public void create(io.atomix.api.value.CreateRequest request, StreamObserver<io.atomix.api.value.CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, value) -> value.openSession(OpenSessionRequest.newBuilder()
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> io.atomix.api.value.CreateResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(response.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void keepAlive(io.atomix.api.value.KeepAliveRequest request, StreamObserver<io.atomix.api.value.KeepAliveResponse> responseObserver) {
    keepAlive.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, value) -> value.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> io.atomix.api.value.KeepAliveResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void close(io.atomix.api.value.CloseRequest request, StreamObserver<io.atomix.api.value.CloseResponse> responseObserver) {
    close.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, value) -> value.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .build()).thenApply(response -> io.atomix.api.value.CloseResponse.newBuilder().build()));
  }

  @Override
  public void set(io.atomix.api.value.SetRequest request, StreamObserver<io.atomix.api.value.SetResponse> responseObserver) {
    set.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, value) -> value.set(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            SetRequest.newBuilder()
                .setValue(request.getValue())
                .build())
            .thenApply(response -> io.atomix.api.value.SetResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
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
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void get(io.atomix.api.value.GetRequest request, StreamObserver<io.atomix.api.value.GetResponse> responseObserver) {
    get.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, value) -> value.get(
            SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            GetRequest.newBuilder().build())
            .thenApply(response -> io.atomix.api.value.GetResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
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
                .setValue(response.getRight().getValue())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void checkAndSet(io.atomix.api.value.CheckAndSetRequest request, StreamObserver<io.atomix.api.value.CheckAndSetResponse> responseObserver) {
    checkAndSet.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, value) -> value.checkAndSet(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            CheckAndSetRequest.newBuilder()
                .setCheck(request.getCheck())
                .setUpdate(request.getUpdate())
                .setVersion(request.getVersion())
                .build())
            .thenApply(response -> io.atomix.api.value.CheckAndSetResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
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
                .setSucceeded(response.getRight().getSucceeded())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void event(io.atomix.api.value.EventRequest request, StreamObserver<io.atomix.api.value.EventResponse> responseObserver) {
    events.<Pair<SessionStreamContext, ListenResponse>>executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, handler, value) -> value.listen(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            ListenRequest.newBuilder().build(),
            handler),
        (partitionId, header, event) -> io.atomix.api.value.EventResponse.newBuilder()
            .setHeader(SessionResponseHeader.newBuilder()
                .setSessionId(header.getSessionId())
                .setPartitionId(partitionId.getPartition())
                .setIndex(event.getLeft().getIndex())
                .setSequenceNumber(event.getLeft().getSequence())
                .addStreams(SessionStreamHeader.newBuilder()
                    .setStreamId(event.getLeft().getStreamId())
                    .setIndex(event.getLeft().getIndex())
                    .setLastItemNumber(event.getLeft().getSequence())
                    .build())
                .build())
            .setType(io.atomix.api.value.EventResponse.Type.valueOf(event.getRight().getType().name()))
            .setPreviousValue(event.getRight().getPreviousValue())
            .setPreviousVersion(event.getRight().getPreviousVersion())
            .setNewValue(event.getRight().getNewValue())
            .setNewVersion(event.getRight().getNewVersion())
            .build());
  }

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.value.CreateRequest, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.value.CreateRequest::getId, r -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.value.KeepAliveRequest, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.value.KeepAliveRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.value.CloseRequest, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.value.CloseRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.value.GetRequest, SessionQueryHeader> GET_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.value.GetRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.value.SetRequest, SessionCommandHeader> SET_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.value.SetRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.value.CheckAndSetRequest, SessionCommandHeader> CHECK_AND_SET_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.value.CheckAndSetRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.value.EventRequest, SessionCommandHeader> EVENT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.value.EventRequest::getId, request -> Collections.singletonList(request.getHeader()));
}
