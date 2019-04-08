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
import io.atomix.core.counter.AsyncAtomicCounter;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.grpc.counter.CasRequest;
import io.atomix.grpc.counter.CasResponse;
import io.atomix.grpc.counter.CounterServiceGrpc;
import io.atomix.grpc.counter.DecrementRequest;
import io.atomix.grpc.counter.DecrementResponse;
import io.atomix.grpc.counter.GetRequest;
import io.atomix.grpc.counter.GetResponse;
import io.atomix.grpc.counter.IncrementRequest;
import io.atomix.grpc.counter.IncrementResponse;
import io.atomix.grpc.counter.SetRequest;
import io.atomix.grpc.counter.SetResponse;
import io.grpc.stub.StreamObserver;

/**
 * Counter service implementation.
 */
public class CounterServiceImpl extends CounterServiceGrpc.CounterServiceImplBase {
  private final PrimitiveExecutor<AtomicCounter, AsyncAtomicCounter> executor;

  public CounterServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, AtomicCounterType.instance(), AtomicCounter::async);
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    executor.execute(request, SetResponse::getDefaultInstance, responseObserver,
        counter -> counter.set(request.getValue())
            .thenApply(v -> SetResponse.newBuilder().build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    executor.execute(request, GetResponse::getDefaultInstance, responseObserver,
        counter -> counter.get()
            .thenApply(value -> GetResponse.newBuilder().setValue(value).build()));
  }

  @Override
  public void cas(CasRequest request, StreamObserver<CasResponse> responseObserver) {
    executor.execute(request, CasResponse::getDefaultInstance, responseObserver,
        counter -> counter.compareAndSet(request.getExpect(), request.getUpdate())
            .thenApply(succeeded -> CasResponse.newBuilder().setSucceeded(succeeded).build()));
  }

  @Override
  public void increment(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
    executor.execute(request, IncrementResponse::getDefaultInstance, responseObserver,
        counter -> counter.addAndGet(request.getDelta() != 0 ? request.getDelta() : 1)
            .thenApply(value -> IncrementResponse.newBuilder().setValue(value).build()));
  }

  @Override
  public void decrement(DecrementRequest request, StreamObserver<DecrementResponse> responseObserver) {
    executor.execute(request, DecrementResponse::getDefaultInstance, responseObserver,
        counter -> counter.addAndGet(-(request.getDelta() != 0 ? request.getDelta() : 1))
            .thenApply(value -> DecrementResponse.newBuilder().setValue(value).build()));
  }
}
