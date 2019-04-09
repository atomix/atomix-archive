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

import io.atomix.core.Atomix;
import io.atomix.core.semaphore.AsyncAtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.AtomicSemaphoreType;
import io.atomix.grpc.semaphore.AcquireRequest;
import io.atomix.grpc.semaphore.AcquireResponse;
import io.atomix.grpc.semaphore.ReleaseRequest;
import io.atomix.grpc.semaphore.ReleaseResponse;
import io.atomix.grpc.semaphore.SemaphoreServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Semaphore service implementation.
 */
public class SemaphoreServiceImpl extends SemaphoreServiceGrpc.SemaphoreServiceImplBase {
  private final PrimitiveExecutor<AtomicSemaphore, AsyncAtomicSemaphore> executor;

  public SemaphoreServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, AtomicSemaphoreType.instance(), AtomicSemaphore::async);
  }

  @Override
  public void acquire(AcquireRequest request, StreamObserver<AcquireResponse> responseObserver) {
    executor.execute(request, AcquireResponse::getDefaultInstance, responseObserver,
        semaphore -> semaphore.acquire(request.getPermits() > 0 ? request.getPermits() : 1)
        .thenApply(version -> AcquireResponse.newBuilder()
            .setVersion(version.value())
            .build()));
  }

  @Override
  public void release(ReleaseRequest request, StreamObserver<ReleaseResponse> responseObserver) {
    executor.execute(request, ReleaseResponse::getDefaultInstance, responseObserver,
        semaphore -> semaphore.release(request.getPermits() > 0 ? request.getPermits() : 1)
            .thenApply(version -> ReleaseResponse.newBuilder().build()));
  }
}
