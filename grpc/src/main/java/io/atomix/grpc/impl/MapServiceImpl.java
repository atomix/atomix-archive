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
import io.atomix.core.map.impl.ContainsKeyResponse;
import io.atomix.core.map.impl.ListenResponse;
import io.atomix.core.map.impl.MapProxy;
import io.atomix.core.map.impl.MapService;
import io.atomix.grpc.headers.SessionCommandHeader;
import io.atomix.grpc.headers.SessionHeader;
import io.atomix.grpc.headers.SessionHeaders;
import io.atomix.grpc.headers.SessionQueryHeader;
import io.atomix.grpc.headers.SessionResponseHeader;
import io.atomix.grpc.headers.SessionResponseHeaders;
import io.atomix.grpc.headers.SessionStreamHeader;
import io.atomix.grpc.map.ClearRequest;
import io.atomix.grpc.map.ClearResponse;
import io.atomix.grpc.map.CloseRequest;
import io.atomix.grpc.map.CloseResponse;
import io.atomix.grpc.map.CreateRequest;
import io.atomix.grpc.map.CreateResponse;
import io.atomix.grpc.map.EventRequest;
import io.atomix.grpc.map.EventResponse;
import io.atomix.grpc.map.ExistsRequest;
import io.atomix.grpc.map.ExistsResponse;
import io.atomix.grpc.map.GetRequest;
import io.atomix.grpc.map.GetResponse;
import io.atomix.grpc.map.KeepAliveRequest;
import io.atomix.grpc.map.KeepAliveResponse;
import io.atomix.grpc.map.MapId;
import io.atomix.grpc.map.MapServiceGrpc;
import io.atomix.grpc.map.PutRequest;
import io.atomix.grpc.map.PutResponse;
import io.atomix.grpc.map.RemoveRequest;
import io.atomix.grpc.map.RemoveResponse;
import io.atomix.grpc.map.ReplaceRequest;
import io.atomix.grpc.map.ReplaceResponse;
import io.atomix.grpc.map.ResponseStatus;
import io.atomix.grpc.map.SizeRequest;
import io.atomix.grpc.map.SizeResponse;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.atomix.grpc.protocol.MultiPrimaryProtocol;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Map service implementation.
 */
public class MapServiceImpl extends MapServiceGrpc.MapServiceImplBase {
  private final PrimitiveFactory<MapProxy, MapId> primitiveFactory;
  private final RequestExecutor<MapProxy, MapId, SessionHeader, CreateRequest, CreateResponse> create;
  private final RequestExecutor<MapProxy, MapId, SessionHeader, KeepAliveRequest, KeepAliveResponse> keepAlive;
  private final RequestExecutor<MapProxy, MapId, SessionHeader, CloseRequest, CloseResponse> close;
  private final RequestExecutor<MapProxy, MapId, SessionQueryHeader, SizeRequest, SizeResponse> size;
  private final RequestExecutor<MapProxy, MapId, SessionCommandHeader, PutRequest, PutResponse> put;
  private final RequestExecutor<MapProxy, MapId, SessionQueryHeader, ExistsRequest, ExistsResponse> exists;
  private final RequestExecutor<MapProxy, MapId, SessionQueryHeader, GetRequest, GetResponse> get;
  private final RequestExecutor<MapProxy, MapId, SessionCommandHeader, ReplaceRequest, ReplaceResponse> replace;
  private final RequestExecutor<MapProxy, MapId, SessionCommandHeader, RemoveRequest, RemoveResponse> remove;
  private final RequestExecutor<MapProxy, MapId, SessionCommandHeader, ClearRequest, ClearResponse> clear;
  private final RequestExecutor<MapProxy, MapId, SessionCommandHeader, EventRequest, EventResponse> events;

  public MapServiceImpl(Atomix atomix) {
    this.primitiveFactory = new PrimitiveFactory<>(atomix, MapService.TYPE, MapProxy::new, MAP_ID_DESCRIPTOR);
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, CloseResponse::getDefaultInstance);
    this.size = new RequestExecutor<>(primitiveFactory, SIZE_DESCRIPTOR, SizeResponse::getDefaultInstance);
    this.put = new RequestExecutor<>(primitiveFactory, PUT_DESCRIPTOR, PutResponse::getDefaultInstance);
    this.exists = new RequestExecutor<>(primitiveFactory, EXISTS_DESCRIPTOR, ExistsResponse::getDefaultInstance);
    this.get = new RequestExecutor<>(primitiveFactory, GET_DESCRIPTOR, GetResponse::getDefaultInstance);
    this.replace = new RequestExecutor<>(primitiveFactory, REPLACE_DESCRIPTOR, ReplaceResponse::getDefaultInstance);
    this.remove = new RequestExecutor<>(primitiveFactory, REMOVE_DESCRIPTOR, RemoveResponse::getDefaultInstance);
    this.clear = new RequestExecutor<>(primitiveFactory, CLEAR_DESCRIPTOR, ClearResponse::getDefaultInstance);
    this.events = new RequestExecutor<>(primitiveFactory, EVENTS_DESCRIPTOR, EventResponse::getDefaultInstance);
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    create.createAll(request, responseObserver, (partitionId, sessionId, map) -> map.openSession(OpenSessionRequest.newBuilder()
        .setSessionId(sessionId)
        .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
            .plusNanos(request.getTimeout().getNanos())
            .toMillis())
        .build())
        .thenApply(response -> SessionHeader.newBuilder()
            .setPartitionId(partitionId.getPartition())
            .build()), (sessionId, headers) -> CreateResponse.newBuilder()
        .setHeaders(SessionHeaders.newBuilder()
            .setSessionId(sessionId)
            .addAllHeaders(headers.collect(Collectors.toList()))
            .build())
        .build());
  }

  @Override
  public void keepAlive(KeepAliveRequest request, StreamObserver<KeepAliveResponse> responseObserver) {
    keepAlive.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(request.getHeaders().getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> SessionHeader.newBuilder()
                .setPartitionId(header.getPartitionId())
                .build()),
        responses -> KeepAliveResponse.newBuilder()
            .setHeaders(SessionHeaders.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .addAllHeaders(responses.collect(Collectors.toList()))
                .build())
            .build());
  }

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    close.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeaders().getSessionId())
            .build()),
        responses -> CloseResponse.newBuilder().build());
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    size.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.size(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.map.impl.SizeRequest.newBuilder().build()),
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
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    put.executeBy(request, request.getKey(), responseObserver,
        (partitionId, header, map) -> map.put(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.map.impl.PutRequest.newBuilder()
                .setKey(request.getKey())
                .setValue(request.getValue())
                .setTtl(request.getTtl())
                .setVersion(request.getVersion())
                .build())
            .thenApply(response -> PutResponse.newBuilder()
                .setHeaders(SessionResponseHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionResponseHeader.newBuilder()
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
                    .build())
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void exists(ExistsRequest request, StreamObserver<ExistsResponse> responseObserver) {
    exists.executeBy(request, request.getKeysList(), responseObserver,
        (header, keys, map) -> map.containsKey(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.map.impl.ContainsKeyRequest.newBuilder()
                .addAllKeys(keys)
                .build()),
        responses -> ExistsResponse.newBuilder()
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
            .setContainsKey(responses.map(Pair::getRight)
                .map(ContainsKeyResponse::getContainsKey)
                .filter(Predicate.isEqual(true))
                .findFirst()
                .orElse(false))
            .build());
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    get.executeBy(request, request.getKey(), responseObserver,
        (partitionId, header, map) -> map.get(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.map.impl.GetRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> GetResponse.newBuilder()
                .setHeaders(SessionResponseHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionResponseHeader.newBuilder()
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
                    .build())
                .setValue(response.getRight().getValue())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void replace(ReplaceRequest request, StreamObserver<ReplaceResponse> responseObserver) {
    replace.executeBy(request, request.getKey(), responseObserver,
        (partitionId, header, map) -> map.replace(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.map.impl.ReplaceRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> ReplaceResponse.newBuilder()
                .setHeaders(SessionResponseHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionResponseHeader.newBuilder()
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
                    .build())
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    remove.executeBy(request, request.getKey(), responseObserver,
        (partitionId, header, map) -> map.remove(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.map.impl.RemoveRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> RemoveResponse.newBuilder()
                .setHeaders(SessionResponseHeaders.newBuilder()
                    .setSessionId(request.getHeaders().getSessionId())
                    .addHeaders(SessionResponseHeader.newBuilder()
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
                    .build())
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    clear.executeAll(request, responseObserver,
        (partitionId, header, map) -> map.clear(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.map.impl.ClearRequest.newBuilder().build()),
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
  public void events(EventRequest request, StreamObserver<EventResponse> responseObserver) {
    events.<Pair<SessionStreamContext, ListenResponse>>executeAll(request, responseObserver,
        (header, handler, map) -> map.listen(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.map.impl.ListenRequest.newBuilder().build(), handler),
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
            .setKey(response.getRight().getKey())
            .setOldValue(response.getRight().getOldValue())
            .setOldVersion(response.getRight().getOldVersion())
            .setNewValue(response.getRight().getNewValue())
            .setNewVersion(response.getRight().getNewVersion())
            .build());
  }

  private static final PrimitiveFactory.PrimitiveIdDescriptor<MapId> MAP_ID_DESCRIPTOR = new PrimitiveFactory.PrimitiveIdDescriptor<MapId>() {
    @Override
    public String getName(MapId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(MapId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(MapId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(MapId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(MapId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(MapId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(MapId id) {
      return id.getLog();
    }
  };

  private static final RequestExecutor.RequestDescriptor<CreateRequest, MapId, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CreateRequest::getId, r -> SessionHeaders.getDefaultInstance());

  private static final RequestExecutor.RequestDescriptor<KeepAliveRequest, MapId, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(KeepAliveRequest::getId, KeepAliveRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<CloseRequest, MapId, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CloseRequest::getId, CloseRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<SizeRequest, MapId, SessionQueryHeader> SIZE_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(SizeRequest::getId, SizeRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<PutRequest, MapId, SessionCommandHeader> PUT_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(PutRequest::getId, PutRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<ExistsRequest, MapId, SessionQueryHeader> EXISTS_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(ExistsRequest::getId, ExistsRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<GetRequest, MapId, SessionQueryHeader> GET_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(GetRequest::getId, GetRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<ReplaceRequest, MapId, SessionCommandHeader> REPLACE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(ReplaceRequest::getId, ReplaceRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<RemoveRequest, MapId, SessionCommandHeader> REMOVE_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(RemoveRequest::getId, RemoveRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<ClearRequest, MapId, SessionCommandHeader> CLEAR_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(ClearRequest::getId, ClearRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<EventRequest, MapId, SessionCommandHeader> EVENTS_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(EventRequest::getId, EventRequest::getHeaders);
}
