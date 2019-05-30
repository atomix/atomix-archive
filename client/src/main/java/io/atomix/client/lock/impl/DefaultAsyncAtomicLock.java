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
import java.util.OptionalLong;
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
import io.atomix.api.lock.LockRequest;
import io.atomix.api.lock.LockResponse;
import io.atomix.api.lock.LockServiceGrpc;
import io.atomix.api.lock.UnlockRequest;
import io.atomix.api.lock.UnlockResponse;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.impl.AbstractManagedPrimitive;
import io.atomix.client.lock.AsyncAtomicLock;
import io.atomix.client.lock.AtomicLock;
import io.atomix.client.utils.concurrent.Scheduled;

/**
 * Raft lock.
 */
public class DefaultAsyncAtomicLock extends AbstractManagedPrimitive<LockServiceGrpc.LockServiceStub, AsyncAtomicLock> implements AsyncAtomicLock {
  private final AtomicLong lockId = new AtomicLong();

  public DefaultAsyncAtomicLock(
      PrimitiveId id,
      PrimitiveManagementService managementService,
      Duration timeout) {
    super(id, LockServiceGrpc.newStub(managementService.getChannelFactory().getChannel()), managementService, timeout);
  }

  @Override
  public CompletableFuture<Long> lock() {
    return command(
        (service, header, observer) -> service.lock(LockRequest.newBuilder()
            .setLockId(getPrimitiveId())
            .setHeader(header)
            .setTimeout(com.google.protobuf.Duration.newBuilder()
                .setSeconds(-1)
                .build())
            .build(), observer), LockResponse::getHeader)
        .thenApply(response -> {
          lockId.set(response.getVersion());
          return response.getVersion();
        });
  }

  @Override
  public CompletableFuture<OptionalLong> tryLock() {
    return command(
        (service, header, observer) -> service.lock(LockRequest.newBuilder()
            .setLockId(getPrimitiveId())
            .setHeader(header)
            .setTimeout(com.google.protobuf.Duration.newBuilder()
                .setSeconds(0)
                .setNanos(0)
                .build())
            .build(), observer), LockResponse::getHeader)
        .thenApply(response -> response.getVersion() > 0 ? OptionalLong.of(response.getVersion()) : OptionalLong.empty())
        .thenApply(version -> {
          if (version.isPresent()) {
            lockId.set(version.getAsLong());
          }
          return version;
        });
  }

  @Override
  public CompletableFuture<OptionalLong> tryLock(Duration timeout) {
    CompletableFuture<OptionalLong> future = new CompletableFuture<>();
    Scheduled timer = context().schedule(timeout, () -> future.complete(OptionalLong.empty()));
    command(
        (service, header, observer) -> service.lock(LockRequest.newBuilder()
            .setLockId(getPrimitiveId())
            .setHeader(header)
            .setTimeout(com.google.protobuf.Duration.newBuilder()
                .setSeconds(timeout.getSeconds())
                .setNanos(timeout.getNano())
                .build())
            .build(), observer), LockResponse::getHeader)
        .thenApply(response -> response.getVersion() > 0 ? OptionalLong.of(response.getVersion()) : OptionalLong.empty())
        .thenAccept(version -> {
          timer.cancel();
          if (!future.isDone()) {
            if (version.isPresent()) {
              lockId.set(version.getAsLong());
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
      return command(
          (service, header, observer) -> service.unlock(UnlockRequest.newBuilder()
              .setLockId(getPrimitiveId())
              .setHeader(header)
              .setVersion(lock)
              .build(), observer), UnlockResponse::getHeader)
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> unlock(long version) {
    return command(
        (service, header, observer) -> service.unlock(UnlockRequest.newBuilder()
            .setLockId(getPrimitiveId())
            .setHeader(header)
            .setVersion(version)
            .build(), observer), UnlockResponse::getHeader)
        .thenApply(response -> response.getUnlocked());
  }

  @Override
  public CompletableFuture<Boolean> isLocked() {
    return isLocked(0);
  }

  @Override
  public CompletableFuture<Boolean> isLocked(long version) {
    return query(
        (service, header, observer) -> service.isLocked(IsLockedRequest.newBuilder()
            .setLockId(getPrimitiveId())
            .setHeader(header)
            .setVersion(version)
            .build(), observer), IsLockedResponse::getHeader)
        .thenApply(response -> response.getIsLocked());
  }

  @Override
  protected CompletableFuture<Long> openSession(Duration timeout) {
    return this.<CreateResponse>session((service, header, observer) -> service.create(CreateRequest.newBuilder()
        .setLockId(getPrimitiveId())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), observer))
        .thenApply(response -> response.getHeader().getSessionId());
  }

  @Override
  protected CompletableFuture<Boolean> keepAlive() {
    return this.<KeepAliveResponse>session((service, header, observer) -> service.keepAlive(KeepAliveRequest.newBuilder()
        .setLockId(getPrimitiveId())
        .build(), observer))
        .thenApply(response -> true);
  }

  @Override
  protected CompletableFuture<Void> close(boolean delete) {
    return this.<CloseResponse>session((service, header, observer) -> service.close(CloseRequest.newBuilder()
        .setLockId(getPrimitiveId())
        .setDelete(delete)
        .build(), observer))
        .thenApply(v -> null);
  }

  @Override
  public AtomicLock sync(Duration operationTimeout) {
    return new BlockingAtomicLock(this, operationTimeout.toMillis());
  }
}