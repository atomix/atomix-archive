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
package io.atomix.server.service.lock;

import java.time.Duration;
import java.util.stream.Collectors;

import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.StreamHeader;
import io.atomix.api.lock.CloseRequest;
import io.atomix.api.lock.CloseResponse;
import io.atomix.api.lock.CreateRequest;
import io.atomix.api.lock.CreateResponse;
import io.atomix.api.lock.IsLockedRequest;
import io.atomix.api.lock.IsLockedResponse;
import io.atomix.api.lock.KeepAliveRequest;
import io.atomix.api.lock.KeepAliveResponse;
import io.atomix.api.lock.LockRequest;
import io.atomix.api.lock.LockResponse;
import io.atomix.api.lock.LockServiceGrpc;
import io.atomix.api.lock.UnlockRequest;
import io.atomix.api.lock.UnlockResponse;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.RequestExecutor;
import io.atomix.server.protocol.ServiceProtocol;
import io.atomix.server.protocol.impl.DefaultSessionClient;
import io.atomix.service.protocol.OpenSessionRequest;
import io.atomix.service.protocol.SessionCommandContext;
import io.atomix.service.protocol.SessionQueryContext;
import io.grpc.stub.StreamObserver;

/**
 * Lock service implementation.
 */
public class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {
  private final RequestExecutor<LockProxy> executor;

  public LockServiceImpl(ServiceProtocol protocol) {
    this.executor = new RequestExecutor<>(new PrimitiveFactory<>(
        protocol.getServiceClient(),
        LockService.TYPE,
        (id, client) -> new LockProxy(new DefaultSessionClient(id, client))));
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), CreateResponse::getDefaultInstance, responseObserver,
        lock -> lock.openSession(OpenSessionRequest.newBuilder()
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
        lock -> lock.keepAlive(io.atomix.service.protocol.KeepAliveRequest.newBuilder()
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
        lock -> lock.closeSession(io.atomix.service.protocol.CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeader().getSessionId())
            .build()).thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void lock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), LockResponse::getDefaultInstance, responseObserver,
        lock -> lock.lock(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.lock.LockRequest.newBuilder()
                .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                    .plusNanos(request.getTimeout().getNanos())
                    .toMillis())
                .build())
            .thenApply(response -> LockResponse.newBuilder()
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
                .setVersion(response.getRight().getAcquired() ? response.getRight().getIndex() : 0)
                .build()));
  }

  @Override
  public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), UnlockResponse::getDefaultInstance, responseObserver,
        lock -> lock.unlock(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.lock.UnlockRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> UnlockResponse.newBuilder()
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
                .setUnlocked(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void isLocked(IsLockedRequest request, StreamObserver<IsLockedResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), IsLockedResponse::getDefaultInstance, responseObserver,
        lock -> lock.isLocked(
            SessionQueryContext.newBuilder()
                .setSessionId(request.getHeader().getSessionId())
                .setLastIndex(request.getHeader().getIndex())
                .setLastSequenceNumber(request.getHeader().getSequenceNumber())
                .build(),
            io.atomix.server.service.lock.IsLockedRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> IsLockedResponse.newBuilder()
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
                .setIsLocked(response.getRight().getLocked())
                .build()));
  }
}
