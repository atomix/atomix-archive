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
package io.atomix.server.lock;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;

import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.headers.SessionStreamHeader;
import io.atomix.api.lock.LockId;
import io.atomix.api.protocol.DistributedLogProtocol;
import io.atomix.api.protocol.MultiPrimaryProtocol;
import io.atomix.api.protocol.MultiRaftProtocol;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.session.impl.DefaultSessionClient;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.PrimitiveIdDescriptor;
import io.atomix.server.impl.RequestExecutor;
import io.grpc.stub.StreamObserver;

/**
 * Lock service implementation.
 */
public class LockServiceImpl extends io.atomix.api.lock.LockServiceGrpc.LockServiceImplBase {
  private final PrimitiveFactory<LockProxy, LockId> primitiveFactory;
  private final RequestExecutor<LockProxy, LockId, SessionHeader, io.atomix.api.lock.CreateRequest, io.atomix.api.lock.CreateResponse> create;
  private final RequestExecutor<LockProxy, LockId, SessionHeader, io.atomix.api.lock.KeepAliveRequest, io.atomix.api.lock.KeepAliveResponse> keepAlive;
  private final RequestExecutor<LockProxy, LockId, SessionHeader, io.atomix.api.lock.CloseRequest, io.atomix.api.lock.CloseResponse> close;
  private final RequestExecutor<LockProxy, LockId, SessionCommandHeader, io.atomix.api.lock.LockRequest, io.atomix.api.lock.LockResponse> lock;
  private final RequestExecutor<LockProxy, LockId, SessionCommandHeader, io.atomix.api.lock.UnlockRequest, io.atomix.api.lock.UnlockResponse> unlock;
  private final RequestExecutor<LockProxy, LockId, SessionQueryHeader, io.atomix.api.lock.IsLockedRequest, io.atomix.api.lock.IsLockedResponse> isLocked;

  public LockServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(
        partitionService,
        LockService.TYPE,
        (id, client) -> new LockProxy(new DefaultSessionClient(id, client)),
        LOCK_ID_DESCRIPTOR);
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, io.atomix.api.lock.CreateResponse::getDefaultInstance);
    this.keepAlive = new RequestExecutor<>(primitiveFactory, KEEP_ALIVE_DESCRIPTOR, io.atomix.api.lock.KeepAliveResponse::getDefaultInstance);
    this.close = new RequestExecutor<>(primitiveFactory, CLOSE_DESCRIPTOR, io.atomix.api.lock.CloseResponse::getDefaultInstance);
    this.lock = new RequestExecutor<>(primitiveFactory, LOCK_DESCRIPTOR, io.atomix.api.lock.LockResponse::getDefaultInstance);
    this.unlock = new RequestExecutor<>(primitiveFactory, UNLOCK_DESCRIPTOR, io.atomix.api.lock.UnlockResponse::getDefaultInstance);
    this.isLocked = new RequestExecutor<>(primitiveFactory, IS_LOCKED_DESCRIPTOR, io.atomix.api.lock.IsLockedResponse::getDefaultInstance);
  }

  @Override
  public void create(io.atomix.api.lock.CreateRequest request, StreamObserver<io.atomix.api.lock.CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, lock) -> lock.openSession(OpenSessionRequest.newBuilder()
            .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                .plusNanos(request.getTimeout().getNanos())
                .toMillis())
            .build())
            .thenApply(response -> io.atomix.api.lock.CreateResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(response.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void keepAlive(io.atomix.api.lock.KeepAliveRequest request, StreamObserver<io.atomix.api.lock.KeepAliveResponse> responseObserver) {
    keepAlive.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.keepAlive(io.atomix.primitive.session.impl.KeepAliveRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .setCommandSequence(header.getLastSequenceNumber())
            .build())
            .thenApply(response -> io.atomix.api.lock.KeepAliveResponse.newBuilder()
                .setHeader(SessionHeader.newBuilder()
                    .setSessionId(header.getSessionId())
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void close(io.atomix.api.lock.CloseRequest request, StreamObserver<io.atomix.api.lock.CloseResponse> responseObserver) {
    close.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.closeSession(io.atomix.primitive.session.impl.CloseSessionRequest.newBuilder()
            .setSessionId(header.getSessionId())
            .build()).thenApply(response -> io.atomix.api.lock.CloseResponse.newBuilder().build()));
  }

  @Override
  public void lock(io.atomix.api.lock.LockRequest request, StreamObserver<io.atomix.api.lock.LockResponse> responseObserver) {
    lock.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.lock(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            LockRequest.newBuilder()
                .setTimeout(Duration.ofSeconds(request.getTimeout().getSeconds())
                    .plusNanos(request.getTimeout().getNanos())
                    .toMillis())
                .build())
            .thenApply(response -> io.atomix.api.lock.LockResponse.newBuilder()
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
  public void unlock(io.atomix.api.lock.UnlockRequest request, StreamObserver<io.atomix.api.lock.UnlockResponse> responseObserver) {
    unlock.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.unlock(
            SessionCommandContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setSequenceNumber(header.getSequenceNumber())
                .build(),
            UnlockRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> io.atomix.api.lock.UnlockResponse.newBuilder()
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
  public void isLocked(io.atomix.api.lock.IsLockedRequest request, StreamObserver<io.atomix.api.lock.IsLockedResponse> responseObserver) {
    isLocked.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, lock) -> lock.isLocked(
            SessionQueryContext.newBuilder()
                .setSessionId(header.getSessionId())
                .setLastIndex(header.getLastIndex())
                .setLastSequenceNumber(header.getLastSequenceNumber())
                .build(),
            IsLockedRequest.newBuilder()
                .setIndex(request.getVersion())
                .build())
            .thenApply(response -> io.atomix.api.lock.IsLockedResponse.newBuilder()
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

  private static final PrimitiveIdDescriptor<LockId> LOCK_ID_DESCRIPTOR = new PrimitiveIdDescriptor<LockId>() {
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

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.lock.CreateRequest, LockId, SessionHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.lock.CreateRequest::getId, r -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.lock.KeepAliveRequest, LockId, SessionHeader> KEEP_ALIVE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.lock.KeepAliveRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.lock.CloseRequest, LockId, SessionHeader> CLOSE_DESCRIPTOR =
      new RequestExecutor.SessionDescriptor<>(io.atomix.api.lock.CloseRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.lock.LockRequest, LockId, SessionCommandHeader> LOCK_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.lock.LockRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.lock.UnlockRequest, LockId, SessionCommandHeader> UNLOCK_DESCRIPTOR =
      new RequestExecutor.SessionCommandDescriptor<>(io.atomix.api.lock.UnlockRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.lock.IsLockedRequest, LockId, SessionQueryHeader> IS_LOCKED_DESCRIPTOR =
      new RequestExecutor.SessionQueryDescriptor<>(io.atomix.api.lock.IsLockedRequest::getId, request -> Collections.singletonList(request.getHeader()));
}
