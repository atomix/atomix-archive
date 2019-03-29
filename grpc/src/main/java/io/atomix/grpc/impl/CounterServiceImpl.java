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
import io.atomix.core.counter.AsyncAtomicCounter;
import io.atomix.grpc.counter.CounterDelta;
import io.atomix.grpc.counter.CounterId;
import io.atomix.grpc.counter.CounterServiceGrpc;
import io.atomix.grpc.counter.CounterSuccess;
import io.atomix.grpc.counter.CounterUpdate;
import io.atomix.grpc.counter.CounterValue;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Counter service implementation.
 */
public class CounterServiceImpl extends CounterServiceGrpc.CounterServiceImplBase {
  private final Atomix atomix;

  public CounterServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(CounterId id) {
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

  private CompletableFuture<AsyncAtomicCounter> getCounter(CounterId id) {
    return atomix.atomicCounterBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(counter -> counter.async());
  }

  private <T> void run(CounterId id, Function<AsyncAtomicCounter, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getCounter(id).whenComplete((counter, getError) -> {
      if (getError == null) {
        function.apply(counter).whenComplete((result, funcError) -> {
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

  private CounterValue toCounterValue(long value) {
    return CounterValue.newBuilder()
        .setValue(value)
        .build();
  }

  private CounterSuccess toCounterSuccess(boolean success) {
    return CounterSuccess.newBuilder()
        .setSuccess(success)
        .build();
  }

  @Override
  public void set(CounterDelta request, StreamObserver<CounterValue> responseObserver) {
    run(request.getId(), counter -> counter.set(request.getDelta()).thenApply(v -> toCounterValue(request.getDelta())), responseObserver);
  }

  @Override
  public void get(CounterId request, StreamObserver<CounterValue> responseObserver) {
    run(request, counter -> counter.get().thenApply(this::toCounterValue), responseObserver);
  }

  @Override
  public void compareAndSet(CounterUpdate request, StreamObserver<CounterSuccess> responseObserver) {
    run(request.getId(), counter -> counter.compareAndSet(request.getExpect(), request.getUpdate()).thenApply(this::toCounterSuccess), responseObserver);
  }

  @Override
  public void incrementAndGet(CounterId request, StreamObserver<CounterValue> responseObserver) {
    run(request, counter -> counter.incrementAndGet().thenApply(this::toCounterValue), responseObserver);
  }

  @Override
  public void decrementAndGet(CounterId request, StreamObserver<CounterValue> responseObserver) {
    run(request, counter -> counter.decrementAndGet().thenApply(this::toCounterValue), responseObserver);
  }

  @Override
  public void getAndIncrement(CounterId request, StreamObserver<CounterValue> responseObserver) {
    run(request, counter -> counter.getAndIncrement().thenApply(this::toCounterValue), responseObserver);
  }

  @Override
  public void getAndDecrement(CounterId request, StreamObserver<CounterValue> responseObserver) {
    run(request, counter -> counter.getAndDecrement().thenApply(this::toCounterValue), responseObserver);
  }

  @Override
  public void getAndAdd(CounterDelta request, StreamObserver<CounterValue> responseObserver) {
    run(request.getId(), counter -> counter.getAndAdd(request.getDelta()).thenApply(this::toCounterValue), responseObserver);
  }

  @Override
  public void addAndGet(CounterDelta request, StreamObserver<CounterValue> responseObserver) {
    run(request.getId(), counter -> counter.getAndAdd(request.getDelta()).thenApply(this::toCounterValue), responseObserver);
  }
}
