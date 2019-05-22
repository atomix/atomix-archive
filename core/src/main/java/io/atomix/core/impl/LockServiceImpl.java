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
package io.atomix.core.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.core.lock.CloseRequest;
import io.atomix.core.lock.CloseResponse;
import io.atomix.core.lock.CreateRequest;
import io.atomix.core.lock.CreateResponse;
import io.atomix.core.lock.IsLockedRequest;
import io.atomix.core.lock.IsLockedResponse;
import io.atomix.core.lock.KeepAliveRequest;
import io.atomix.core.lock.KeepAliveResponse;
import io.atomix.core.lock.LockId;
import io.atomix.core.lock.LockRequest;
import io.atomix.core.lock.LockResponse;
import io.atomix.core.lock.LockServiceGrpc;
import io.atomix.core.lock.UnlockRequest;
import io.atomix.core.lock.UnlockResponse;
import io.atomix.core.lock.impl.LockProxy;
import io.atomix.core.lock.impl.LockService;
import io.atomix.grpc.headers.SessionCommandHeader;
import io.atomix.grpc.headers.SessionHeader;
import io.atomix.grpc.headers.SessionQueryHeader;
import io.atomix.grpc.headers.SessionResponseHeader;
import io.atomix.grpc.headers.SessionStreamHeader;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.atomix.grpc.protocol.MultiPrimaryProtocol;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.session.impl.DefaultSessionClient;
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

  public LockServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(
        partitionService,
        LockService.TYPE,
        (id, client) -> new LockProxy(new DefaultSessionClient(id, client)),
        LOCK_ID_DESCRIPTOR);
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
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(sessionId)
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void keepAlive(KeepAliveRequest request, StreamObserver<KeepAliveResponse> responseObserver) {
    keepAlive.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> KeepAliveResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    close.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .build()).thenApply(response -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void lock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
    lock.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.lock(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.lock.impl.LockRequest.newBuilder()
                .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                    .plusNanos(request.getTimeout().getNanos())
                    .toMillis())
                .build())
            .thenApply(response -> LockResponse.newBuilder()
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
                .setVersion(response.getRight().getAcquired() ? response.getRight().getIndex() : 0)
                .build()));
  }

  @Override
  public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
    unlock.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.unlock(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            io.atomix.core.lock.impl.UnlockRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> UnlockResponse.newBuilder()
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
                .setUnlocked(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void isLocked(IsLockedRequest request, StreamObserver<IsLockedResponse> responseObserver) {
    isLocked.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.isLocked(
            SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            io.atomix.core.lock.impl.IsLockedRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> IsLockedResponse.newBuilder()
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
      new RequestExecutor.SessionDescriptor<>(CreateRequest::getId, r -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<KeepAliveRequest, LockId, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(KeepAliveRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<CloseRequest, LockId, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(CloseRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<LockRequest, LockId, SessionCommandHeader> LOCK_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(LockRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<UnlockRequest, LockId, SessionCommandHeader> UNLOCK_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(UnlockRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<IsLockedRequest, LockId, SessionQueryHeader> IS_LOCKED_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(IsLockedRequest::getId, request -> Collections.singletonList(request.getHeader()));
}
