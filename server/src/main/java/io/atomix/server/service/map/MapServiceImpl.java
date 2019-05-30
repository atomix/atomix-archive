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
package io.atomix.server.service.map;

import java.time.Duration;
import java.util.stream.Collectors;

import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.StreamHeader;
import io.atomix.api.map.ClearRequest;
import io.atomix.api.map.ClearResponse;
import io.atomix.api.map.CloseRequest;
import io.atomix.api.map.CloseResponse;
import io.atomix.api.map.CreateRequest;
import io.atomix.api.map.CreateResponse;
import io.atomix.api.map.EventRequest;
import io.atomix.api.map.EventResponse;
import io.atomix.api.map.ExistsRequest;
import io.atomix.api.map.ExistsResponse;
import io.atomix.api.map.GetRequest;
import io.atomix.api.map.GetResponse;
import io.atomix.api.map.KeepAliveRequest;
import io.atomix.api.map.KeepAliveResponse;
import io.atomix.api.map.MapServiceGrpc;
import io.atomix.api.map.PutRequest;
import io.atomix.api.map.PutResponse;
import io.atomix.api.map.RemoveRequest;
import io.atomix.api.map.RemoveResponse;
import io.atomix.api.map.ReplaceRequest;
import io.atomix.api.map.ReplaceResponse;
import io.atomix.api.map.ResponseStatus;
import io.atomix.api.map.SizeRequest;
import io.atomix.api.map.SizeResponse;
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
 * Map service implementation.
 */
public class MapServiceImpl extends MapServiceGrpc.MapServiceImplBase {
  private final RequestExecutor<MapProxy> executor;

  public MapServiceImpl(ServiceProtocol protocol) {
    this.executor = new RequestExecutor<>(new PrimitiveFactory<>(
        protocol.getServiceClient(),
        MapService.TYPE,
        (id, client) -> new MapProxy(new DefaultSessionClient(id, client))));
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), CreateResponse::getDefaultInstance, responseObserver,
        map -> map.openSession(OpenSessionRequest.newBuilder()
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
        map -> map.keepAlive(io.atomix.service.protocol.KeepAliveRequest.newBuilder()
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
        map -> map.closeSession(CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeader().getSessionId())
            .build())
            .thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), SizeResponse::getDefaultInstance, responseObserver,
        map -> map.size(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getIndex())
                .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.map.SizeRequest.newBuilder().build())
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
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), PutResponse::getDefaultInstance, responseObserver,
        map -> map.put(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.map.PutRequest.newBuilder()
                .setKey(request.getKey())
                .setValue(request.getValue())
                .setTtl(request.getTtl())
                .setVersion(request.getVersion())
                .build())
            .thenApply(response -> PutResponse.newBuilder()
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
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void exists(ExistsRequest request, StreamObserver<ExistsResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), ExistsResponse::getDefaultInstance, responseObserver,
        map -> map.containsKey(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getIndex())
                .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            ContainsKeyRequest.newBuilder()
                .addKeys(request.getKey())
                .build())
            .thenApply(response -> ExistsResponse.newBuilder()
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
                .setContainsKey(response.getRight().getContainsKey())
                .build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), GetResponse::getDefaultInstance, responseObserver,
        map -> map.get(SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getIndex())
                .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.map.GetRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> GetResponse.newBuilder()
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
                .setValue(response.getRight().getValue())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void replace(ReplaceRequest request, StreamObserver<ReplaceResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), ReplaceResponse::getDefaultInstance, responseObserver,
        map -> map.replace(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.map.ReplaceRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> ReplaceResponse.newBuilder()
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
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), RemoveResponse::getDefaultInstance, responseObserver,
        map -> map.remove(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.map.RemoveRequest.newBuilder()
                .setKey(request.getKey())
                .build())
            .thenApply(response -> RemoveResponse.newBuilder()
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
                .setStatus(ResponseStatus.valueOf(response.getRight().getStatus().name()))
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .build()));
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), ClearResponse::getDefaultInstance, responseObserver,
        map -> map.clear(SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.map.ClearRequest.newBuilder().build())
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
  public void events(EventRequest request, StreamObserver<EventResponse> responseObserver) {
    executor.<Pair<SessionStreamContext, ListenResponse>, EventResponse>execute(request.getHeader().getName(), EventResponse::getDefaultInstance, responseObserver,
        (map, handler) -> map.listen(SessionCommandContext.newBuilder()
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
            .setKey(response.getRight().getKey())
            .setOldValue(response.getRight().getOldValue())
            .setOldVersion(response.getRight().getOldVersion())
            .setNewValue(response.getRight().getNewValue())
            .setNewVersion(response.getRight().getNewVersion())
            .build());
  }
}
