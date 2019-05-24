/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.client.lock.impl;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import io.atomix.api.lock.CloseRequest;
import io.atomix.api.lock.CloseResponse;
import io.atomix.api.lock.CreateRequest;
import io.atomix.api.lock.CreateResponse;
import io.atomix.api.lock.IsLockedRequest;
import io.atomix.api.lock.IsLockedResponse;
import io.atomix.api.lock.KeepAliveRequest;
import io.atomix.api.lock.KeepAliveResponse;
import io.atomix.api.lock.LockId;
import io.atomix.api.lock.LockRequest;
import io.atomix.api.lock.LockResponse;
import io.atomix.api.lock.LockServiceGrpc;
import io.atomix.api.lock.UnlockRequest;
import io.atomix.api.lock.UnlockResponse;
import io.atomix.api.protocol.DistributedLogProtocol;
import io.atomix.api.protocol.MultiPrimaryProtocol;
import io.atomix.api.protocol.MultiRaftProtocol;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.channel.ChannelFactory;
import io.atomix.client.impl.AbstractAsyncPrimitive;
import io.atomix.client.impl.PrimitiveIdDescriptor;
import io.atomix.client.impl.PrimitivePartition;
import io.atomix.client.lock.AsyncAtomicLock;
import io.atomix.client.lock.AtomicLock;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.time.Version;

/**
 * Raft lock.
 */
public class DefaultAsyncAtomicLock extends AbstractAsyncPrimitive<LockId, AsyncAtomicLock> implements AsyncAtomicLock {
  private final LockServiceGrpc.LockServiceStub lock;
  private final AtomicLong lockId = new AtomicLong();

  public DefaultAsyncAtomicLock(LockId id, ChannelFactory channelFactory, PrimitiveManagementService managementService, Partitioner<String> partitioner, Duration timeout) {
    super(id, LOCK_ID_DESCRIPTOR, managementService, partitioner, timeout);
    this.lock = LockServiceGrpc.newStub(channelFactory.getChannel());
  }

  @Override
  public CompletableFuture<Version> lock() {
    PrimitivePartition partition = getPartition();
    return this.<LockResponse>execute(observer -> lock.lock(LockRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(-1)
            .build())
        .build(), observer))
        .thenCompose(response -> partition.order(new Version(response.getVersion()), response.getHeader()))
        .thenApply(version -> {
          lockId.set(version.value());
          return version;
        });
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock() {
    PrimitivePartition partition = getPartition();
    return this.<LockResponse>execute(observer -> lock.lock(LockRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(0)
            .setNanos(0)
            .build())
        .build(), observer))
        .thenCompose(response -> partition.order(
            Optional.ofNullable(response.getVersion() > 0
                ? new Version(response.getVersion())
                : null),
            response.getHeader()))
        .thenApply(version -> {
          if (version.isPresent()) {
            lockId.set(version.get().value());
          }
          return version;
        });
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock(Duration timeout) {
    CompletableFuture<Optional<Version>> future = new CompletableFuture<>();
    Scheduled timer = context().schedule(timeout, () -> future.complete(Optional.empty()));
    PrimitivePartition partition = getPartition();
    this.<LockResponse>execute(observer -> lock.lock(LockRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), observer))
        .thenCompose(response -> partition.order(
            Optional.ofNullable(response.getVersion() > 0
                ? new Version(response.getVersion())
                : null),
            response.getHeader()))
        .thenAccept(version -> {
          timer.cancel();
          if (!future.isDone()) {
            if (version.isPresent()) {
              lockId.set(version.get().value());
            }
            future.complete(version);
          }
        })
        .whenComplete((result, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> unlock() {
    // Use the current lock ID to ensure we only unlock the lock currently held by this process.
    long lock = this.lockId.getAndSet(0);
    if (lock != 0) {
      PrimitivePartition partition = getPartition();
      return this.<UnlockResponse>execute(observer -> this.lock.unlock(UnlockRequest.newBuilder()
          .setId(id())
          .setHeader(partition.getCommandHeader())
          .setVersion(lock)
          .build(), observer))
          .thenCompose(response -> partition.order(null, response.getHeader()));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> unlock(Version version) {
    PrimitivePartition partition = getPartition();
    return this.<UnlockResponse>execute(observer -> this.lock.unlock(UnlockRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setVersion(version.value())
        .build(), observer))
        .thenCompose(response -> partition.order(response.getUnlocked(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Boolean> isLocked() {
    return isLocked(new Version(0));
  }

  @Override
  public CompletableFuture<Boolean> isLocked(Version version) {
    PrimitivePartition partition = getPartition();
    return this.<IsLockedResponse>execute(observer -> this.lock.isLocked(IsLockedRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getQueryHeader())
        .setVersion(version.value())
        .build(), observer))
        .thenCompose(response -> partition.order(response.getIsLocked(), response.getHeader()));
  }

  @Override
  public CompletableFuture<AsyncAtomicLock> connect() {
    return this.<CreateResponse>execute(stream -> lock.create(CreateRequest.newBuilder()
        .setId(id())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), stream))
        .thenAccept(response -> {
          startKeepAlive(response.getHeader());
        })
        .thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<Void> keepAlive() {
    PrimitivePartition partition = getPartition();
    return this.<KeepAliveResponse>execute(stream -> lock.keepAlive(KeepAliveRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getSessionHeader())
        .build(), stream))
        .thenAccept(response -> completeKeepAlive(response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> close() {
    PrimitivePartition partition = getPartition();
    return this.<CloseResponse>execute(stream -> lock.close(CloseRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getSessionHeader())
        .build(), stream))
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public AtomicLock sync(Duration operationTimeout) {
    return new BlockingAtomicLock(this, operationTimeout.toMillis());
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
}