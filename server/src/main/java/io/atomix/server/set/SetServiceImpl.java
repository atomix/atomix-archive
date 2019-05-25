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
package io.atomix.server.set;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.headers.SessionStreamHeader;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.session.impl.CloseSessionRequest;
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
 * Set service implementation.
 */
public class SetServiceImpl extends io.atomix.api.set.SetServiceGrpc.SetServiceImplBase {
  private final PrimitiveFactory<SetProxy> primitiveFactory;
  private final RequestExecutor<SetProxy, SessionHeader, io.atomix.api.set.CreateRequest, io.atomix.api.set.CreateResponse> create;
  private final RequestExecutor<SetProxy, SessionHeader, io.atomix.api.set.KeepAliveRequest, io.atomix.api.set.KeepAliveResponse> keepAlive;
  private final RequestExecutor<SetProxy, SessionHeader, io.atomix.api.set.CloseRequest, io.atomix.api.set.CloseResponse> close;
  private final RequestExecutor<SetProxy, SessionCommandHeader, io.atomix.api.set.AddRequest, io.atomix.api.set.AddResponse> add;
  private final RequestExecutor<SetProxy, SessionCommandHeader, io.atomix.api.set.RemoveRequest, io.atomix.api.set.RemoveResponse> remove;
  private final RequestExecutor<SetProxy, SessionQueryHeader, io.atomix.api.set.ContainsRequest, io.atomix.api.set.ContainsResponse> contains;
  private final RequestExecutor<SetProxy, SessionQueryHeader, io.atomix.api.set.SizeRequest, io.atomix.api.set.SizeResponse> size;
  private final RequestExecutor<SetProxy, SessionCommandHeader, io.atomix.api.set.ClearRequest, io.atomix.api.set.ClearResponse> clear;
  private final RequestExecutor<SetProxy, SessionCommandHeader, io.atomix.api.set.EventRequest, io.atomix.api.set.EventResponse> listen;
  private final RequestExecutor<SetProxy, SessionQueryHeader, io.atomix.api.set.IterateRequest, io.atomix.api.set.IterateResponse> iterate;

  public SetServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(
        partitionService,
        SetService.TYPE,
        (id, client) -> new SetProxy(new DefaultSessionClient(id, client)));
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, io.atomix.api.set.CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, io.atomix.api.set.KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, io.atomix.api.set.CloseResponse::getDefaultInstance);
    this.add = new RequestExecutor<>(primitiveFactory, ADD_DESCRIPTOR, io.atomix.api.set.AddResponse::getDefaultInstance);
    this.remove = new RequestExecutor<>(primitiveFactory, REMOVE_DESCRIPTOR, io.atomix.api.set.RemoveResponse::getDefaultInstance);
    this.contains = new RequestExecutor<>(primitiveFactory, CONTAINS_DESCRIPTOR, io.atomix.api.set.ContainsResponse::getDefaultInstance);
    this.size = new RequestExecutor<>(primitiveFactory, SIZE_DESCRIPTOR, io.atomix.api.set.SizeResponse::getDefaultInstance);
    this.clear = new RequestExecutor<>(primitiveFactory, CLEAR_DESCRIPTOR, io.atomix.api.set.ClearResponse::getDefaultInstance);
    this.listen = new RequestExecutor<>(primitiveFactory, EVENT_DESCRIPTOR, io.atomix.api.set.EventResponse::getDefaultInstance);
    this.iterate = new RequestExecutor<>(primitiveFactory, ITERATE_DESCRIPTOR, io.atomix.api.set.IterateResponse::getDefaultInstance);
  }

  @Override
  public void create(io.atomix.api.set.CreateRequest request, StreamObserver<io.atomix.api.set.CreateResponse> responseObserver) {
    create.createAll(request, responseObserver,
        (partitionId, set) -> set.openSession(OpenSessionRequest.newBuilder()
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> SessionHeader.newBuilder()
                .setSessionId(response.getSessionId())
                .setPartitionId(partitionId.getPartition())
                .build()),
        responses -> io.atomix.api.set.CreateResponse.newBuilder()
            .addAllHeaders(responses)
            .build());
  }

  @Override
  public void keepAlive(io.atomix.api.set.KeepAliveRequest request, StreamObserver<io.atomix.api.set.KeepAliveResponse> responseObserver) {
    keepAlive.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> SessionHeader.newBuilder()
                .setSessionId(header.getSessionId())
                .setPartitionId(partitionId.getPartition())
                .build()),
        responses -> io.atomix.api.set.KeepAliveResponse.newBuilder()
            .addAllHeaders(responses)
            .build());
  }

  @Override
  public void close(io.atomix.api.set.CloseRequest request, StreamObserver<io.atomix.api.set.CloseResponse> responseObserver) {
    close.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.closeSession(CloseSessionRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .build()),
        responses -> io.atomix.api.set.CloseResponse.newBuilder().build());
  }

  @Override
  public void add(io.atomix.api.set.AddRequest request, StreamObserver<io.atomix.api.set.AddResponse> responseObserver) {
    add.executeBy(request, request.getValuesList(), responseObserver,
        (partitionId, header, values, set) -> set.add(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            AddRequest.newBuilder()
                .addAllValues(values)
                .build())
            .thenApply(response -> io.atomix.api.set.AddResponse.newBuilder()
                .setAdded(response.getRight().getAdded())
                .addHeaders(SessionResponseHeader.newBuilder()
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
                .build()),
        responses -> io.atomix.api.set.AddResponse.newBuilder()
            .addAllHeaders(responses.stream().map(response -> response.getHeaders(0)).collect(Collectors.toList()))
            .setAdded(responses.stream().map(response -> response.getAdded())
                .reduce(Boolean::logicalOr)
                .orElse(false))
            .build());
  }

  @Override
  public void remove(io.atomix.api.set.RemoveRequest request, StreamObserver<io.atomix.api.set.RemoveResponse> responseObserver) {
    remove.executeBy(request, request.getValuesList(), responseObserver,
        (partitionId, header, values, set) -> set.remove(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            RemoveRequest.newBuilder()
                .addAllValues(values)
                .build())
            .thenApply(response -> io.atomix.api.set.RemoveResponse.newBuilder()
                .setRemoved(response.getRight().getRemoved())
                .addHeaders(SessionResponseHeader.newBuilder()
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
                .build()),
        responses -> io.atomix.api.set.RemoveResponse.newBuilder()
            .addAllHeaders(responses.stream().map(response -> response.getHeaders(0)).collect(Collectors.toList()))
            .setRemoved(responses.stream().map(response -> response.getRemoved())
                .reduce(Boolean::logicalOr)
                .orElse(false))
            .build());
  }

  @Override
  public void contains(io.atomix.api.set.ContainsRequest request, StreamObserver<io.atomix.api.set.ContainsResponse> responseObserver) {
    contains.executeBy(request, request.getValuesList(), responseObserver,
        (partitionId, header, values, set) -> set.contains(SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            ContainsRequest.newBuilder()
                .addAllValues(values)
                .build())
            .thenApply(response -> io.atomix.api.set.ContainsResponse.newBuilder()
                .setContains(response.getRight().getContains())
                .addHeaders(SessionResponseHeader.newBuilder()
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
                .build()),
        responses -> io.atomix.api.set.ContainsResponse.newBuilder()
            .addAllHeaders(responses.stream().map(response -> response.getHeaders(0)).collect(Collectors.toList()))
            .setContains(responses.stream().map(response -> response.getContains())
                .reduce(Boolean::logicalAnd)
                .orElse(false))
            .build());
  }

  @Override
  public void size(io.atomix.api.set.SizeRequest request, StreamObserver<io.atomix.api.set.SizeResponse> responseObserver) {
    size.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.size(SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            SizeRequest.newBuilder().build())
            .thenApply(response -> io.atomix.api.set.SizeResponse.newBuilder()
                .setSize(response.getRight().getSize())
                .addHeaders(SessionResponseHeader.newBuilder()
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
                .build()),
        responses -> io.atomix.api.set.SizeResponse.newBuilder()
            .addAllHeaders(responses.stream().map(response -> response.getHeaders(0)).collect(Collectors.toList()))
            .setSize(responses.stream().mapToInt(r -> r.getSize()).sum())
            .build());
  }

  @Override
  public void clear(io.atomix.api.set.ClearRequest request, StreamObserver<io.atomix.api.set.ClearResponse> responseObserver) {
    clear.executeAll(request, responseObserver,
        (partitionId, header, set) -> set.clear(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            ClearRequest.newBuilder().build())
            .thenApply(response -> io.atomix.api.set.ClearResponse.newBuilder()
                .addHeaders(SessionResponseHeader.newBuilder()
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
                .build()),
        responses -> io.atomix.api.set.ClearResponse.newBuilder()
            .addAllHeaders(responses.stream().map(response -> response.getHeaders(0)).collect(Collectors.toList()))
            .build());
  }

  @Override
  public void listen(io.atomix.api.set.EventRequest request, StreamObserver<io.atomix.api.set.EventResponse> responseObserver) {
    listen.<Pair<SessionStreamContext, ListenResponse>>executeAll(request, responseObserver,
        (header, handler, set) -> set.listen(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            ListenRequest.newBuilder().build(), handler),
        (header, response) -> io.atomix.api.set.EventResponse.newBuilder()
            .setHeader(SessionResponseHeader.newBuilder()
                .setSessionId(header.getSessionId())
                .setPartitionId(header.getPartitionId())
                .setIndex(response.getLeft().getIndex())
                .setSequenceNumber(response.getLeft().getSequence())
                .addStreams(SessionStreamHeader.newBuilder()
                    .setStreamId(response.getLeft().getStreamId())
                    .setIndex(response.getLeft().getIndex())
                    .setLastItemNumber(response.getLeft().getSequence())
                    .build())
                .build())
            .setType(io.atomix.api.set.EventResponse.Type.valueOf(response.getRight().getType().name()))
            .setValue(response.getRight().getValue())
            .build());
  }

  @Override
  public void iterate(io.atomix.api.set.IterateRequest request, StreamObserver<io.atomix.api.set.IterateResponse> responseObserver) {
    iterate.<Pair<SessionStreamContext, IterateResponse>>executeAll(request, responseObserver,
        (header, handler, set) -> set.iterate(SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            IterateRequest.newBuilder().build(), handler),
        (header, response) -> io.atomix.api.set.IterateResponse.newBuilder()
            .setHeader(SessionResponseHeader.newBuilder()
                .setSessionId(header.getSessionId())
                .setPartitionId(header.getPartitionId())
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

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.CreateRequest, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.set.CreateRequest::getId, r -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.KeepAliveRequest, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.set.KeepAliveRequest::getId, io.atomix.api.set.KeepAliveRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.CloseRequest, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.set.CloseRequest::getId, io.atomix.api.set.CloseRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.AddRequest, SessionCommandHeader> ADD_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.set.AddRequest::getId, io.atomix.api.set.AddRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.RemoveRequest, SessionCommandHeader> REMOVE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.set.RemoveRequest::getId, io.atomix.api.set.RemoveRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.ContainsRequest, SessionQueryHeader> CONTAINS_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.set.ContainsRequest::getId, io.atomix.api.set.ContainsRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.SizeRequest, SessionQueryHeader> SIZE_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.set.SizeRequest::getId, io.atomix.api.set.SizeRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.ClearRequest, SessionCommandHeader> CLEAR_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.set.ClearRequest::getId, io.atomix.api.set.ClearRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.EventRequest, SessionCommandHeader> EVENT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.set.EventRequest::getId, io.atomix.api.set.EventRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.set.IterateRequest, SessionQueryHeader> ITERATE_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.set.IterateRequest::getId, io.atomix.api.set.IterateRequest::getHeadersList);
}
