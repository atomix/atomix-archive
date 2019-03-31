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
import io.atomix.grpc.counter.CasRequest;
import io.atomix.grpc.counter.CasResponse;
import io.atomix.grpc.counter.CounterId;
import io.atomix.grpc.counter.CounterServiceGrpc;
import io.atomix.grpc.counter.DecrementRequest;
import io.atomix.grpc.counter.DecrementResponse;
import io.atomix.grpc.counter.GetRequest;
import io.atomix.grpc.counter.GetResponse;
import io.atomix.grpc.counter.IncrementRequest;
import io.atomix.grpc.counter.IncrementResponse;
import io.atomix.grpc.counter.SetRequest;
import io.atomix.grpc.counter.SetResponse;
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
      return MultiPrimaryProtocol.builder(id.getMultiPrimary().getGroup())
          .build();
    } else if (id.hasLog()) {
      return DistributedLogProtocol.builder(id.getLog().getGroup())
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

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    run(request.getId(), counter -> counter.set(request.getValue())
        .thenApply(v -> SetResponse.newBuilder().build()), responseObserver);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    run(request.getId(), counter -> counter.get()
        .thenApply(value -> GetResponse.newBuilder().setValue(value).build()), responseObserver);
  }

  @Override
  public void cas(CasRequest request, StreamObserver<CasResponse> responseObserver) {
    run(request.getId(), counter -> counter.compareAndSet(request.getExpect(), request.getUpdate())
        .thenApply(succeeded -> CasResponse.newBuilder().setSucceeded(succeeded).build()), responseObserver);
  }

  @Override
  public void increment(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
    run(request.getId(), counter -> counter.addAndGet(request.getDelta() != 0 ? request.getDelta() : 1)
        .thenApply(value -> IncrementResponse.newBuilder().setValue(value).build()), responseObserver);
  }

  @Override
  public void decrement(DecrementRequest request, StreamObserver<DecrementResponse> responseObserver) {
    run(request.getId(), counter -> counter.addAndGet(-(request.getDelta() != 0 ? request.getDelta() : 1))
        .thenApply(value -> DecrementResponse.newBuilder().setValue(value).build()), responseObserver);
  }
}
