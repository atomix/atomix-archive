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

import io.atomix.core.Atomix;
import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AtomicLock;
import io.atomix.core.lock.AtomicLockType;
import io.atomix.grpc.lock.IsLockedRequest;
import io.atomix.grpc.lock.IsLockedResponse;
import io.atomix.grpc.lock.LockRequest;
import io.atomix.grpc.lock.LockResponse;
import io.atomix.grpc.lock.LockServiceGrpc;
import io.atomix.grpc.lock.UnlockRequest;
import io.atomix.grpc.lock.UnlockResponse;
import io.atomix.utils.time.Version;
import io.grpc.stub.StreamObserver;

/**
 * Lock service implementation.
 */
public class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {
  private final PrimitiveExecutor<AtomicLock, AsyncAtomicLock> executor;

  public LockServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, AtomicLockType.instance(), AtomicLock::async);
  }

  @Override
  public void lock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
    if (request.hasTimeout()) {
      if (request.getTimeout().getSeconds() == 0 && request.getTimeout().getNanos() == 0) {
        executor.execute(request, LockResponse::getDefaultInstance, responseObserver,
            lock -> lock.tryLock()
                .thenApply(result -> LockResponse.newBuilder()
                    .setVersion(result.isPresent() ? result.get().value() : 0)
                    .build()));
      } else {
        Duration timeout = Duration.ofSeconds(request.getTimeout().getSeconds())
            .plusNanos(request.getTimeout().getNanos());
        executor.execute(request, LockResponse::getDefaultInstance, responseObserver,
            lock -> lock.tryLock(timeout)
                .thenApply(result -> LockResponse.newBuilder()
                    .setVersion(result.isPresent() ? result.get().value() : 0)
                    .build()));
      }
    } else {
      executor.execute(request, LockResponse::getDefaultInstance, responseObserver,
          lock -> lock.lock().thenApply(version -> LockResponse.newBuilder()
              .setVersion(version.value())
              .build()));
    }
  }

  @Override
  public void unlock(UnlockRequest request, StreamObserver<UnlockResponse> responseObserver) {
    executor.execute(request, UnlockResponse::getDefaultInstance, responseObserver,
        lock -> lock.unlock(new Version(request.getVersion()))
            .thenApply(succeeded -> UnlockResponse.newBuilder()
                .setUnlocked(succeeded)
                .build()));
  }

  @Override
  public void isLocked(IsLockedRequest request, StreamObserver<IsLockedResponse> responseObserver) {
    if (request.getVersion() > 0) {
      executor.execute(request, IsLockedResponse::getDefaultInstance, responseObserver,
          lock -> lock.isLocked(new Version(request.getVersion()))
              .thenApply(isLocked -> IsLockedResponse.newBuilder()
                  .setIsLocked(isLocked)
                  .build()));
    } else {
      executor.execute(request, IsLockedResponse::getDefaultInstance, responseObserver,
          lock -> lock.isLocked()
              .thenApply(isLocked -> IsLockedResponse.newBuilder()
                  .setIsLocked(isLocked)
                  .build()));
    }
  }
}
