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
package io.atomix.server.map;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.headers.SessionStreamHeader;
import io.atomix.api.map.ResponseStatus;
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
 * Map service implementation.
 */
public class MapServiceImpl extends io.atomix.api.map.MapServiceGrpc.MapServiceImplBase {
  private final PrimitiveFactory<MapProxy> primitiveFactory;
  private final RequestExecutor<MapProxy, SessionHeader, io.atomix.api.map.CreateRequest, io.atomix.api.map.CreateResponse> create;
  private final RequestExecutor<MapProxy, SessionHeader, io.atomix.api.map.KeepAliveRequest, io.atomix.api.map.KeepAliveResponse> keepAlive;
  private final RequestExecutor<MapProxy, SessionHeader, io.atomix.api.map.CloseRequest, io.atomix.api.map.CloseResponse> close;
  private final RequestExecutor<MapProxy, SessionQueryHeader, io.atomix.api.map.SizeRequest, io.atomix.api.map.SizeResponse> size;
  private final RequestExecutor<MapProxy, SessionCommandHeader, io.atomix.api.map.PutRequest, io.atomix.api.map.PutResponse> put;
  private final RequestExecutor<MapProxy, SessionQueryHeader, io.atomix.api.map.ExistsRequest, io.atomix.api.map.ExistsResponse> exists;
  private final RequestExecutor<MapProxy, SessionQueryHeader, io.atomix.api.map.GetRequest, io.atomix.api.map.GetResponse> get;
  private final RequestExecutor<MapProxy, SessionCommandHeader, io.atomix.api.map.ReplaceRequest, io.atomix.api.map.ReplaceResponse> replace;
  private final RequestExecutor<MapProxy, SessionCommandHeader, io.atomix.api.map.RemoveRequest, io.atomix.api.map.RemoveResponse> remove;
  private final RequestExecutor<MapProxy, SessionCommandHeader, io.atomix.api.map.ClearRequest, io.atomix.api.map.ClearResponse> clear;
  private final RequestExecutor<MapProxy, SessionCommandHeader, io.atomix.api.map.EventRequest, io.atomix.api.map.EventResponse> events;

  public MapServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(partitionService, MapService.TYPE, (id, client) -> new MapProxy(new DefaultSessionClient(id, client)));
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, io.atomix.api.map.CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, io.atomix.api.map.KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, io.atomix.api.map.CloseResponse::getDefaultInstance);
    this.size = new RequestExecutor<>(primitiveFactory, SIZE_DESCRIPTOR, io.atomix.api.map.SizeResponse::getDefaultInstance);
    this.put = new RequestExecutor<>(primitiveFactory, PUT_DESCRIPTOR, io.atomix.api.map.PutResponse::getDefaultInstance);
    this.exists = new RequestExecutor<>(primitiveFactory, EXISTS_DESCRIPTOR, io.atomix.api.map.ExistsResponse::getDefaultInstance);
    this.get = new RequestExecutor<>(primitiveFactory, GET_DESCRIPTOR, io.atomix.api.map.GetResponse::getDefaultInstance);
    this.replace = new RequestExecutor<>(primitiveFactory, REPLACE_DESCRIPTOR, io.atomix.api.map.ReplaceResponse::getDefaultInstance);
    this.remove = new RequestExecutor<>(primitiveFactory, REMOVE_DESCRIPTOR, io.atomix.api.map.RemoveResponse::getDefaultInstance);
    this.clear = new RequestExecutor<>(primitiveFactory, CLEAR_DESCRIPTOR, io.atomix.api.map.ClearResponse::getDefaultInstance);
    this.events = new RequestExecutor<>(primitiveFactory, EVENTS_DESCRIPTOR, io.atomix.api.map.EventResponse::getDefaultInstance);
  }

  @Override
  public void create(io.atomix.api.map.CreateRequest request, StreamObserver<io.atomix.api.map.CreateResponse> responseObserver) {
    create.createAll(request, responseObserver,
        (partitionId, map) -> map.openSession(OpenSessionRequest.newBuilder()
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> SessionHeader.newBuilder()
                .setSessionId(response.getSessionId())
                .setPartitionId(partitionId.getPartition())
                .build()),
        responses -> io.atomix.api.map.CreateResponse.newBuilder()
            .addAllHeaders(responses)
            .build());
  }

  @Override
  public void keepAlive(io.atomix.api.map.KeepAliveRequest request, StreamObserver<io.atomix.api.map.KeepAliveResponse> responseObserver) {
    keepAlive.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> SessionHeader.newBuilder()
                .setSessionId(header.getSessionId())
                .setPartitionId(partitionId.getPartition())
                .build()),
        responses -> io.atomix.api.map.KeepAliveResponse.newBuilder()
            .addAllHeaders(responses)
            .build());
  }

  @Override
  public void close(io.atomix.api.map.CloseRequest request, StreamObserver<io.atomix.api.map.CloseResponse> responseObserver) {
    close.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.closeSession(CloseSessionRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .build()),
        responses -> io.atomix.api.map.CloseResponse.newBuilder().build());
  }

  @Override
  public void size(io.atomix.api.map.SizeRequest request, StreamObserver<io.atomix.api.map.SizeResponse> responseObserver) {
    size.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.size(SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            SizeRequest.newBuilder().build())
            .thenApply(response -> io.atomix.api.map.SizeResponse.newBuilder()
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
        responses -> io.atomix.api.map.SizeResponse.newBuilder()
            .addAllHeaders(responses.stream().map(response -> response.getHeaders(0)).collect(Collectors.toList()))
            .setSize(responses.stream().mapToInt(r -> r.getSize()).sum())
            .build());
  }

  @Override
  public void put(io.atomix.api.map.PutRequest request, StreamObserver<io.atomix.api.map.PutResponse> responseObserver) {
    put.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, map) -> map.put(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            PutRequest.newBuilder()
                .setKey(request.getKey())
                .setValue(request.getValue())
                .setTtl(request.getTtl())
                .setVersion(request.getVersion())
                .build())
            .thenApply(response -> io.atomix.api.map.PutResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(header.getPartitionId())
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
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void exists(io.atomix.api.map.ExistsRequest request, StreamObserver<io.atomix.api.map.ExistsResponse> responseObserver) {
    exists.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, map) -> map.containsKey(SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            ContainsKeyRequest.newBuilder()
                .addKeys(request.getKey())
                .build())
            .thenApply(response -> io.atomix.api.map.ExistsResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(header.getPartitionId())
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
                .setContainsKey(response.getRight().getContainsKey())
                .build()));
  }

  @Override
  public void get(io.atomix.api.map.GetRequest request, StreamObserver<io.atomix.api.map.GetResponse> responseObserver) {
    get.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, map) -> map.get(SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            GetRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> io.atomix.api.map.GetResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(header.getPartitionId())
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
  public void replace(io.atomix.api.map.ReplaceRequest request, StreamObserver<io.atomix.api.map.ReplaceResponse> responseObserver) {
    replace.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, map) -> map.replace(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            ReplaceRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> io.atomix.api.map.ReplaceResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(header.getPartitionId())
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
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void remove(io.atomix.api.map.RemoveRequest request, StreamObserver<io.atomix.api.map.RemoveResponse> responseObserver) {
    remove.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, map) -> map.remove(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            RemoveRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> io.atomix.api.map.RemoveResponse.newBuilder()
                .setHeader(SessionResponseHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(header.getPartitionId())
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
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void clear(io.atomix.api.map.ClearRequest request, StreamObserver<io.atomix.api.map.ClearResponse> responseObserver) {
    clear.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.clear(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            ClearRequest.newBuilder().build())
            .thenApply(response -> io.atomix.api.map.ClearResponse.newBuilder()
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
        responses -> io.atomix.api.map.ClearResponse.newBuilder()
            .addAllHeaders(responses.stream().map(response -> response.getHeaders(0)).collect(Collectors.toList()))
            .build());
  }

  @Override
  public void events(io.atomix.api.map.EventRequest request, StreamObserver<io.atomix.api.map.EventResponse> responseObserver) {
    events.<Pair<SessionStreamContext, ListenResponse>>executeAll(request, responseObserver,
        (header, handler, map) -> map.listen(SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            ListenRequest.newBuilder().build(), handler),
        (header, response) -> io.atomix.api.map.EventResponse.newBuilder()
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
            .setType(io.atomix.api.map.EventResponse.Type.valueOf(response.getRight().getType().name()))
            .setKey(response.getRight().getKey())
            .setOldValue(response.getRight().getOldValue())
            .setOldVersion(response.getRight().getOldVersion())
            .setNewValue(response.getRight().getNewValue())
            .setNewVersion(response.getRight().getNewVersion())
            .build());
  }

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.CreateRequest, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.map.CreateRequest::getId, r -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.KeepAliveRequest, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.map.KeepAliveRequest::getId, io.atomix.api.map.KeepAliveRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.CloseRequest, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.map.CloseRequest::getId, io.atomix.api.map.CloseRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.SizeRequest, SessionQueryHeader> SIZE_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.map.SizeRequest::getId, io.atomix.api.map.SizeRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.PutRequest, SessionCommandHeader> PUT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.map.PutRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.ExistsRequest, SessionQueryHeader> EXISTS_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.map.ExistsRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.GetRequest, SessionQueryHeader> GET_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.map.GetRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.ReplaceRequest, SessionCommandHeader> REPLACE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.map.ReplaceRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.RemoveRequest, SessionCommandHeader> REMOVE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.map.RemoveRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.ClearRequest, SessionCommandHeader> CLEAR_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.map.ClearRequest::getId, io.atomix.api.map.ClearRequest::getHeadersList);

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.map.EventRequest, SessionCommandHeader> EVENTS_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.map.EventRequest::getId, io.atomix.api.map.EventRequest::getHeadersList);
}
