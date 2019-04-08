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

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.grpc.value.CasRequest;
import io.atomix.grpc.value.CasResponse;
import io.atomix.grpc.value.GetRequest;
import io.atomix.grpc.value.GetResponse;
import io.atomix.grpc.value.SetRequest;
import io.atomix.grpc.value.SetResponse;
import io.atomix.grpc.value.ValueId;
import io.atomix.grpc.value.ValueServiceGrpc;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Counter service implementation.
 */
public class ValueServiceImpl extends ValueServiceGrpc.ValueServiceImplBase {
  private final Atomix atomix;

  public ValueServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(ValueId id) {
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

  private CompletableFuture<AsyncAtomicValue<byte[]>> getValue(ValueId id) {
    return atomix.<byte[]>atomicValueBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(value -> value.async());
  }

  private <T> void run(ValueId id, Function<AsyncAtomicValue<byte[]>, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getValue(id).whenComplete((counter, getError) -> {
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
    run(request.getId(), value -> value.set(request.getValue().toByteArray())
        .thenApply(result -> SetResponse.newBuilder()
            .setVersion(result.version())
            .build()), responseObserver);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    run(request.getId(), value -> value.get()
        .thenApply(result -> GetResponse.newBuilder()
            .setValue(result.value() != null ? ByteString.copyFrom(result.value()) : ByteString.EMPTY)
            .setVersion(result.version())
            .build()), responseObserver);
  }

  @Override
  public void cas(CasRequest request, StreamObserver<CasResponse> responseObserver) {
    if (!request.getExpect().isEmpty()) {
      run(request.getId(), value -> value.compareAndSet(request.getExpect().toByteArray(), request.getUpdate().toByteArray())
          .thenApply(result -> CasResponse.newBuilder()
              .setSucceeded(result.isPresent())
              .setVersion(result.isPresent() ? result.get().version() : 0)
              .build()), responseObserver);
    } else {
      run(request.getId(), value -> value.compareAndSet(request.getVersion(), request.getUpdate().toByteArray())
          .thenApply(result -> CasResponse.newBuilder()
              .setSucceeded(result.isPresent())
              .setVersion(result.isPresent() ? result.get().version() : 0)
              .build()), responseObserver);
    }
  }
}
