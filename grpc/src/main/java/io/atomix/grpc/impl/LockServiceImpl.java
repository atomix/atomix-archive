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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.atomix.core.Atomix;
import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.grpc.lock.IsLocked;
import io.atomix.grpc.lock.Lock;
import io.atomix.grpc.lock.LockId;
import io.atomix.grpc.lock.LockServiceGrpc;
import io.atomix.grpc.lock.LockTimeout;
import io.atomix.grpc.lock.Unlocked;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.utils.time.Version;
import io.grpc.stub.StreamObserver;

/**
 * Lock service implementation.
 */
public class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {
  private final Atomix atomix;

  public LockServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(LockId id) {
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

  private CompletableFuture<AsyncAtomicLock> getLock(LockId id) {
    return atomix.atomicLockBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(lock -> lock.async());
  }

  private <T> void run(LockId id, Function<AsyncAtomicLock, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getLock(id).whenComplete((map, getError) -> {
      if (getError == null) {
        function.apply(map).whenComplete((result, funcError) -> {
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

  private Lock toLock(Version version) {
    return Lock.newBuilder()
        .setVersion(version.value())
        .build();
  }

  private Lock toLock(Optional<Version> version) {
    return version.isPresent() ? toLock(version.get()) : null;
  }

  private IsLocked toIsLocked(boolean isLocked) {
    return IsLocked.newBuilder()
        .setIsLocked(isLocked)
        .build();
  }

  private Unlocked toUnlocked(boolean unlocked) {
    return Unlocked.newBuilder()
        .setUnlocked(unlocked)
        .build();
  }

  @Override
  public void lock(LockTimeout request, StreamObserver<Lock> responseObserver) {
    if (request.hasTimeout()) {
      Duration timeout = Duration.ofSeconds(request.getTimeout().getSeconds())
          .plusNanos(request.getTimeout().getNanos());
      run(request.getId(), lock -> lock.tryLock(timeout).thenApply(this::toLock), responseObserver);
    } else {
      run(request.getId(), lock -> lock.lock().thenApply(this::toLock), responseObserver);
    }
  }

  @Override
  public void unlock(Lock request, StreamObserver<Unlocked> responseObserver) {
    run(request.getId(), lock -> lock.unlock(new Version(request.getVersion())).thenApply(this::toUnlocked), responseObserver);
  }

  @Override
  public void isLocked(Lock request, StreamObserver<IsLocked> responseObserver) {
    if (request.getVersion() > 0) {
      run(request.getId(), lock -> lock.isLocked(new Version(request.getVersion())).thenApply(this::toIsLocked), responseObserver);
    } else {
      run(request.getId(), lock -> lock.isLocked().thenApply(this::toIsLocked), responseObserver);
    }
  }
}
