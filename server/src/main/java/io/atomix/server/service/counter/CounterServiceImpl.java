/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.server.service.counter;

import io.atomix.api.counter.CheckAndSetRequest;
import io.atomix.api.counter.CheckAndSetResponse;
import io.atomix.api.counter.CloseRequest;
import io.atomix.api.counter.CloseResponse;
import io.atomix.api.counter.CounterServiceGrpc;
import io.atomix.api.counter.CreateRequest;
import io.atomix.api.counter.CreateResponse;
import io.atomix.api.counter.DecrementRequest;
import io.atomix.api.counter.DecrementResponse;
import io.atomix.api.counter.GetRequest;
import io.atomix.api.counter.GetResponse;
import io.atomix.api.counter.IncrementRequest;
import io.atomix.api.counter.IncrementResponse;
import io.atomix.api.counter.SetRequest;
import io.atomix.api.counter.SetResponse;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.RequestExecutor;
import io.atomix.server.protocol.ServiceProtocol;
import io.atomix.server.protocol.impl.DefaultServiceClient;
import io.atomix.service.protocol.RequestContext;
import io.grpc.stub.StreamObserver;

/**
 * Counter service implementation.
 */
public class CounterServiceImpl extends CounterServiceGrpc.CounterServiceImplBase {
  private final RequestExecutor<CounterProxy> executor;

  public CounterServiceImpl(ServiceProtocol protocol) {
    this.executor = new RequestExecutor<>(new PrimitiveFactory<>(
        protocol.getServiceClient(),
        CounterService.TYPE,
        (id, client) -> new CounterProxy(new DefaultServiceClient(id, client))));
  }

  @Override
  public void create(CreateRequest request, StreamObserver<CreateResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), CreateResponse::getDefaultInstance, responseObserver,
        counter -> counter.create()
            .thenApply(v -> CreateResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder().build())
                .build()));
  }

  @Override
  public void close(CloseRequest request, StreamObserver<CloseResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), CloseResponse::getDefaultInstance, responseObserver,
        counter -> counter.delete()
            .thenApply(v -> CloseResponse.newBuilder().build()));
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), SetResponse::getDefaultInstance, responseObserver,
        counter -> counter.set(
            RequestContext.newBuilder()
                .setIndex(request.getHeader().getIndex())
                .build(),
            io.atomix.server.service.counter.SetRequest.newBuilder()
                .setValue(request.getValue())
                .build())
            .thenApply(response -> SetResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), GetResponse::getDefaultInstance, responseObserver,
        counter -> counter.get(
            RequestContext.newBuilder()
                .setIndex(request.getHeader().getIndex())
                .build(),
            io.atomix.server.service.counter.GetRequest.newBuilder().build())
            .thenApply(response -> GetResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setValue(response.getRight().getValue())
                .build()));
  }

  @Override
  public void checkAndSet(CheckAndSetRequest request, StreamObserver<CheckAndSetResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), CheckAndSetResponse::getDefaultInstance, responseObserver,
        counter -> counter.checkAndSet(
            RequestContext.newBuilder()
                .setIndex(request.getHeader().getIndex())
                .build(),
            io.atomix.server.service.counter.CheckAndSetRequest.newBuilder()
                .setExpect(request.getExpect())
                .setUpdate(request.getUpdate())
                .build())
            .thenApply(response -> CheckAndSetResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void increment(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), IncrementResponse::getDefaultInstance, responseObserver,
        counter -> counter.increment(
            RequestContext.newBuilder()
                .setIndex(request.getHeader().getIndex())
                .build(),
            io.atomix.server.service.counter.IncrementRequest.newBuilder()
                .setDelta(request.getDelta())
                .build())
            .thenApply(response -> IncrementResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setPreviousValue(response.getRight().getPreviousValue())
                .setNextValue(response.getRight().getNextValue())
                .build()));
  }

  @Override
  public void decrement(DecrementRequest request, StreamObserver<DecrementResponse> responseObserver) {
    executor.execute(request.getHeader().getName(), DecrementResponse::getDefaultInstance, responseObserver,
        counter -> counter.decrement(RequestContext.newBuilder()
                .setIndex(request.getHeader().getIndex())
                .build(),
            io.atomix.server.service.counter.DecrementRequest.newBuilder()
                .setDelta(request.getDelta())
                .build())
            .thenApply(response -> DecrementResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setPreviousValue(response.getRight().getPreviousValue())
                .setNextValue(response.getRight().getNextValue())
                .build()));
  }
}
