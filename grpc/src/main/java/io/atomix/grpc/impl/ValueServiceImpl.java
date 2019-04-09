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

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueType;
import io.atomix.grpc.value.CasRequest;
import io.atomix.grpc.value.CasResponse;
import io.atomix.grpc.value.GetRequest;
import io.atomix.grpc.value.GetResponse;
import io.atomix.grpc.value.SetRequest;
import io.atomix.grpc.value.SetResponse;
import io.atomix.grpc.value.ValueServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Counter service implementation.
 */
public class ValueServiceImpl extends ValueServiceGrpc.ValueServiceImplBase {
  private final PrimitiveExecutor<AtomicValue<byte[]>, AsyncAtomicValue<byte[]>> executor;

  public ValueServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, AtomicValueType.instance(), AtomicValue::async);
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    executor.execute(request, SetResponse::getDefaultInstance, responseObserver,
        value -> value.set(request.getValue().toByteArray())
            .thenApply(result -> SetResponse.newBuilder()
                .setVersion(result.version())
                .build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    executor.execute(request, GetResponse::getDefaultInstance, responseObserver,
        value -> value.get()
            .thenApply(result -> GetResponse.newBuilder()
                .setValue(result.value() != null ? ByteString.copyFrom(result.value()) : ByteString.EMPTY)
                .setVersion(result.version())
                .build()));
  }

  @Override
  public void cas(CasRequest request, StreamObserver<CasResponse> responseObserver) {
    if (!request.getExpect().isEmpty()) {
      executor.execute(request, CasResponse::getDefaultInstance, responseObserver,
          value -> value.compareAndSet(request.getExpect().toByteArray(), request.getUpdate().toByteArray())
              .thenApply(result -> CasResponse.newBuilder()
                  .setSucceeded(result.isPresent())
                  .setVersion(result.isPresent() ? result.get().version() : 0)
                  .build()));
    } else {
      executor.execute(request, CasResponse::getDefaultInstance, responseObserver,
          value -> value.compareAndSet(request.getVersion(), request.getUpdate().toByteArray())
              .thenApply(result -> CasResponse.newBuilder()
                  .setSucceeded(result.isPresent())
                  .setVersion(result.isPresent() ? result.get().version() : 0)
                  .build()));
    }
  }
}
