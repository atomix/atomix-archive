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
package io.atomix.server.service.value;

import java.time.Duration;
import java.util.stream.Collectors;

import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.StreamHeader;
import io.atomix.api.value.CheckAndSetRequest;
import io.atomix.api.value.CheckAndSetResponse;
import io.atomix.api.value.CloseRequest;
import io.atomix.api.value.CloseResponse;
import io.atomix.api.value.CreateRequest;
import io.atomix.api.value.CreateResponse;
import io.atomix.api.value.EventRequest;
import io.atomix.api.value.EventResponse;
import io.atomix.api.value.GetRequest;
import io.atomix.api.value.GetResponse;
import io.atomix.api.value.KeepAliveRequest;
import io.atomix.api.value.KeepAliveResponse;
import io.atomix.api.value.SetRequest;
import io.atomix.api.value.SetResponse;
import io.atomix.api.value.ValueServiceGrpc;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.RequestExecutor;
import io.atomix.server.protocol.ServiceProtocol;
import io.atomix.server.protocol.impl.DefaultSessionClient;
import io.atomix.service.protocol.OpenSessionRequest;
import io.atomix.service.protocol.SessionCommandContext;
import io.atomix.service.protocol.SessionQueryContext;
import io.atomix.service.protocol.SessionStreamContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Value service implementation.
 */
public class ValueServiceImpl extends ValueServiceGrpc.ValueServiceImplBase {
  private final RequestExecutor<ValueProxy> executor;

  public ValueServiceImpl(ServiceProtocol protocol) {
    this.executor = new RequestExecutor<>(new PrimitiveFactory<>(
        protocol.getServiceClient(),
        ValueService.TYPE,
        (id, client) -> new ValueProxy(new DefaultSessionClient(id, client))));
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), CreateResponse::getDefaultInstance, responseObserver,
        value -> value.openSession(OpenSessionRequest.newBuilder()
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
        value -> value.keepAlive(io.atomix.service.protocol.KeepAliveRequest.newBuilder()
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
        value -> value.closeSession(io.atomix.service.protocol.CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeader().getSessionId())
            .build()).thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), SetResponse::getDefaultInstance, responseObserver,
        value -> value.set(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.value.SetRequest.newBuilder()
                .setValue(request.getValue())
                .build())
            .thenApply(response -> SetResponse.newBuilder()
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
                .setPreviousValue(response.getRight().getPreviousValue())
                .setPreviousVersion(response.getRight().getPreviousVersion())
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), GetResponse::getDefaultInstance, responseObserver,
        value -> value.get(
            SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getIndex())
                .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.value.GetRequest.newBuilder().build())
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
  public void checkAndSet(CheckAndSetRequest request, StreamObserver<CheckAndSetResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), CheckAndSetResponse::getDefaultInstance, responseObserver,
        value -> value.checkAndSet(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.value.CheckAndSetRequest.newBuilder()
                .setCheck(request.getCheck())
                .setUpdate(request.getUpdate())
                .setVersion(request.getVersion())
                .build())
            .thenApply(response -> CheckAndSetResponse.newBuilder()
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
                .setVersion(response.getRight().getVersion())
                .build()));
  }

  @Override
  public void event(EventRequest request, StreamObserver<EventResponse> responseObserver) {
    executor.<Pair<SessionStreamContext, ListenResponse>, EventResponse>execute(request.getHeader().getName(), EventResponse::getDefaultInstance, responseObserver,
        (value, handler) -> value.listen(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            ListenRequest.newBuilder().build(),
            handler),
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
            .setPreviousValue(response.getRight().getPreviousValue())
            .setPreviousVersion(response.getRight().getPreviousVersion())
            .setNewValue(response.getRight().getNewValue())
            .setNewVersion(response.getRight().getNewVersion())
            .build());
  }
}
