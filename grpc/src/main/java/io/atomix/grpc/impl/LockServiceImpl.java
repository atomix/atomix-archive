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
import io.atomix.core.lock.impl.LockProxy;
import io.atomix.core.lock.impl.LockService;
import io.atomix.grpc.headers.SessionCommandHeader;
import io.atomix.grpc.headers.SessionHeader;
import io.atomix.grpc.headers.SessionHeaders;
import io.atomix.grpc.headers.SessionQueryHeader;
import io.atomix.grpc.headers.SessionResponseHeader;
import io.atomix.grpc.headers.SessionResponseHeaders;
import io.atomix.grpc.headers.SessionStreamHeader;
import io.atomix.grpc.lock.CloseRequest;
import io.atomix.grpc.lock.CloseResponse;
import io.atomix.grpc.lock.CreateRequest;
import io.atomix.grpc.lock.CreateResponse;
import io.atomix.grpc.lock.IsLockedRequest;
import io.atomix.grpc.lock.IsLockedResponse;
import io.atomix.grpc.lock.KeepAliveRequest;
import io.atomix.grpc.lock.KeepAliveResponse;
import io.atomix.grpc.lock.LockId;
import io.atomix.grpc.lock.LockRequest;
import io.atomix.grpc.lock.LockResponse;
import io.atomix.grpc.lock.LockServiceGrpc;
import io.atomix.grpc.lock.UnlockRequest;
import io.atomix.grpc.lock.UnlockResponse;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.atomix.grpc.protocol.MultiPrimaryProtocol;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.grpc.stub.StreamObserver;

/**
 * Lock service implementation.
 */
public class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {
  private final PrimitiveFactory<LockProxy, LockId> primitiveFactory;
  private final RequestExecutor<LockProxy, LockId, SessionHeader, CreateRequest, CreateResponse> create;
  private final RequestExecutor<LockProxy, LockId, SessionHeader, KeepAliveRequest, KeepAliveResponse> keepAlive;
  private final RequestExecutor<LockProxy, LockId, SessionHeader, CloseRequest, CloseResponse> close;
  private final RequestExecutor<LockProxy, LockId, SessionCommandHeader, LockRequest, LockResponse> lock;
  private final RequestExecutor<LockProxy, LockId, SessionCommandHeader, UnlockRequest, UnlockResponse> unlock;
  private final RequestExecutor<LockProxy, LockId, SessionQueryHeader, IsLockedRequest, IsLockedResponse> isLocked;

  public LockServiceImpl(Atomix atomix) {
    this.primitiveFactory = new PrimitiveFactory<>(atomix, LockService.TYPE, LockProxy::new, LOCK_ID_DESCRIPTOR);
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, CloseResponse::getDefaultInstance);
    this.lock = new RequestExecutor<>(primitiveFactory, LOCK_DESCRIPTOR, LockResponse::getDefaultInstance);
    this.unlock = new RequestExecutor<>(primitiveFactory, UNLOCK_DESCRIPTOR, UnlockResponse::getDefaultInstance);
    this.isLocked = new RequestExecutor<>(primitiveFactory, IS_LOCKED_DESCRIPTOR, IsLockedResponse::getDefaultInstance);
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, sessionId, lock) -> lock.openSession(OpenSessionRequest.newBuilder()
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
        (partitionId, header, lock) -> lock.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
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
        (partitionId, header, lock) -> lock.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(request.getHeaders().getSessionId())
            .build()).thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void lock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
    lock.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, lock) -> lock.lock(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.lock.impl.LockRequest.newBuilder()
                .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                    .plusNanos(request.getTimeout().getNanos())
                    .toMillis())
                .build())
            .thenApply(response -> LockResponse.newBuilder()
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
                .setVersion(response.getRight().getAcquired() ? response.getRight().getIndex() : 0)
                .build()));
  }

  @Override
  public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
    unlock.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, lock) -> lock.unlock(
            SessionCommandContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.lock.impl.UnlockRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> UnlockResponse.newBuilder()
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
                .setUnlocked(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void isLocked(IsLockedRequest request, StreamObserver<IsLockedResponse> responseObserver) {
    isLocked.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, lock) -> lock.isLocked(
            SessionQueryContext.newBuilder()
                .setSessionId(request.getHeaders().getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.lock.impl.IsLockedRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> IsLockedResponse.newBuilder()
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
                .setIsLocked(response.getRight().getLocked())
                .build()));
  }

  private static final PrimitiveFactory.PrimitiveIdDescriptor<LockId> LOCK_ID_DESCRIPTOR = new PrimitiveFactory.PrimitiveIdDescriptor<LockId>() {
    @Override
    public String getName(LockId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(LockId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(LockId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(LockId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(LockId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(LockId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(LockId id) {
      return id.getLog();
    }
  };

  private static final RequestExecutor.RequestDescriptor<CreateRequest, LockId, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CreateRequest::getId, r -> SessionHeaders.getDefaultInstance());

  private static final RequestExecutor.RequestDescriptor<KeepAliveRequest, LockId, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(KeepAliveRequest::getId, KeepAliveRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<CloseRequest, LockId, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CloseRequest::getId, CloseRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<LockRequest, LockId, SessionCommandHeader> LOCK_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(LockRequest::getId, LockRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<UnlockRequest, LockId, SessionCommandHeader> UNLOCK_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(UnlockRequest::getId, UnlockRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<IsLockedRequest, LockId, SessionQueryHeader> IS_LOCKED_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(IsLockedRequest::getId, IsLockedRequest::getHeaders);
}
