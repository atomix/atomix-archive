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
package io.atomix.core.lock.impl;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AtomicLock;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.impl.SessionEnabledAsyncPrimitive;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.time.Version;

/**
 * Raft lock.
 */
public class DefaultAsyncAtomicLock extends SessionEnabledAsyncPrimitive<LockProxy, AsyncAtomicLock> implements AsyncAtomicLock {
  private final AtomicLong lock = new AtomicLong();

  public DefaultAsyncAtomicLock(LockProxy proxy, Duration timeout, PrimitiveManagementService managementService) {
    super(proxy, timeout, managementService);
  }

  @Override
  public CompletableFuture<Version> lock() {
    return execute(LockProxy::lock, LockRequest.newBuilder()
        .setTimeout(-1)
        .build())
        .thenApply(response -> {
          lock.set(response.getIndex());
          return new Version(response.getIndex());
        });
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock() {
    // If the proxy is currently disconnected from the cluster, we can just fail the lock attempt here.
    PrimitiveState state = getState();
    if (state != PrimitiveState.CONNECTED) {
      return CompletableFuture.completedFuture(Optional.empty());
    }

    return execute(LockProxy::lock, LockRequest.newBuilder()
        .setTimeout(0)
        .build())
        .thenApply(response -> {
          if (response.getAcquired()) {
            lock.set(response.getIndex());
            return Optional.of(new Version(response.getIndex()));
          }
          return Optional.empty();
        });
  }

  @Override
  public CompletableFuture<Optional<Version>> tryLock(Duration timeout) {
    CompletableFuture<Optional<Version>> future = new CompletableFuture<>();
    Scheduled timer = getProxy().context().schedule(timeout, () -> future.complete(Optional.empty()));
    execute(LockProxy::lock, LockRequest.newBuilder()
        .setTimeout(timeout.toMillis())
        .build())
        .thenAccept(response -> {
          timer.cancel();
          if (!future.isDone()) {
            if (response.getAcquired()) {
              lock.set(response.getIndex());
              future.complete(Optional.of(new Version(response.getIndex())));
            } else {
              future.complete(Optional.empty());
            }
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
    long lock = this.lock.getAndSet(0);
    if (lock != 0) {
      return execute(LockProxy::unlock, UnlockRequest.newBuilder()
          .setIndex(lock)
          .build())
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> unlock(Version version) {
    return execute(LockProxy::unlock, UnlockRequest.newBuilder()
        .setIndex(version.value())
        .build())
        .thenApply(response -> response.getSucceeded());
  }

  @Override
  public CompletableFuture<Boolean> isLocked() {
    return isLocked(new Version(0));
  }

  @Override
  public CompletableFuture<Boolean> isLocked(Version version) {
    return execute(LockProxy::isLocked, IsLockedRequest.newBuilder()
        .setIndex(version.value())
        .build())
        .thenApply(response -> response.getLocked());
  }

  @Override
  public AtomicLock sync(Duration operationTimeout) {
    return new BlockingAtomicLock(this, operationTimeout.toMillis());
  }
}