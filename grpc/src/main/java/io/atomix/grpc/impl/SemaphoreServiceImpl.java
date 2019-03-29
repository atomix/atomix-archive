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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.atomix.core.Atomix;
import io.atomix.core.semaphore.AsyncAtomicSemaphore;
import io.atomix.grpc.semaphore.AcquireRequest;
import io.atomix.grpc.semaphore.AcquireResponse;
import io.atomix.grpc.semaphore.ReleaseRequest;
import io.atomix.grpc.semaphore.ReleaseResponse;
import io.atomix.grpc.semaphore.SemaphoreId;
import io.atomix.grpc.semaphore.SemaphoreServiceGrpc;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Semaphore service implementation.
 */
public class SemaphoreServiceImpl extends SemaphoreServiceGrpc.SemaphoreServiceImplBase {
  private final Atomix atomix;

  public SemaphoreServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(SemaphoreId id) {
    if (id.hasRaft()) {
      return MultiRaftProtocol.builder(id.getRaft().getGroup())
          .build();
    } else if (id.hasMultiPrimary()) {
      return MultiPrimaryProtocol.builder(id.getRaft().getGroup())
          .build();
    } else if (id.hasLog()) {
      return DistributedLogProtocol.builder(id.getRaft().getGroup())
          .build();
    }
    return null;
  }

  private CompletableFuture<AsyncAtomicSemaphore> getSemaphore(SemaphoreId id) {
    return atomix.atomicSemaphoreBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(semaphore -> semaphore.async());
  }

  private <T> void run(SemaphoreId id, Function<AsyncAtomicSemaphore, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getSemaphore(id).whenComplete((semaphore, getError) -> {
      if (getError == null) {
        function.apply(semaphore).whenComplete((result, funcError) -> {
          if (funcError == null) {
            responseObserver.onNext(result);
            responseObserver.onCompleted();
          } else {
            responseObserver.onError(funcError);
            responseObserver.onCompleted();
          }
        });
      } else {
        responseObserver.onError(getError);
        responseObserver.onCompleted();
      }
    });
  }

  @Override
  public void acquire(AcquireRequest request, StreamObserver<AcquireResponse> responseObserver) {
    run(request.getId(), semaphore -> semaphore.acquire(request.getPermits() > 0 ? request.getPermits() : 1)
        .thenApply(version -> AcquireResponse.newBuilder()
            .setVersion(version.value())
            .build()), responseObserver);
  }

  @Override
  public void release(ReleaseRequest request, StreamObserver<ReleaseResponse> responseObserver) {
    run(request.getId(), semaphore -> semaphore.release(request.getPermits() > 0 ? request.getPermits() : 1)
        .thenApply(version -> ReleaseResponse.newBuilder().build()), responseObserver);
  }
}
