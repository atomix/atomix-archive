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
import io.atomix.core.counter.impl.CounterProxy;
import io.atomix.core.counter.impl.CounterService;
import io.atomix.grpc.counter.CheckAndSetRequest;
import io.atomix.grpc.counter.CheckAndSetResponse;
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
import io.atomix.grpc.headers.RequestHeader;
import io.atomix.grpc.headers.ResponseHeader;
import io.atomix.grpc.headers.ResponseHeaders;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.atomix.grpc.protocol.MultiPrimaryProtocol;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.primitive.service.impl.RequestContext;
import io.grpc.stub.StreamObserver;

/**
 * Counter service implementation.
 */
public class CounterServiceImpl extends CounterServiceGrpc.CounterServiceImplBase {
  private final PrimitiveFactory<CounterProxy, CounterId> primitiveFactory;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, SetRequest, SetResponse> set;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, GetRequest, GetResponse> get;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, CheckAndSetRequest, CheckAndSetResponse> checkAndSet;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, IncrementRequest, IncrementResponse> increment;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, DecrementRequest, DecrementResponse> decrement;

  public CounterServiceImpl(Atomix atomix) {
    this.primitiveFactory = new PrimitiveFactory<>(atomix, CounterService.TYPE, CounterProxy::new, COUNTER_ID_DESCRIPTOR);
    this.set = new RequestExecutor<>(primitiveFactory, SET_DESCRIPTOR, SetResponse::getDefaultInstance);
    this.get = new RequestExecutor<>(primitiveFactory, GET_DESCRIPTOR, GetResponse::getDefaultInstance);
    this.checkAndSet = new RequestExecutor<>(primitiveFactory, CHECK_AND_SET_DESCRIPTOR, CheckAndSetResponse::getDefaultInstance);
    this.increment = new RequestExecutor<>(primitiveFactory, INCREMENT_DESCRIPTOR, IncrementResponse::getDefaultInstance);
    this.decrement = new RequestExecutor<>(primitiveFactory, DECREMENT_DESCRIPTOR, DecrementResponse::getDefaultInstance);
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    set.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, counter) -> counter.set(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            io.atomix.core.counter.impl.SetRequest.newBuilder()
                .setValue(request.getValue())
                .build())
            .thenApply(response -> SetResponse.newBuilder()
                .setHeaders(ResponseHeaders.newBuilder()
                    .addHeaders(ResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
                        .setIndex(response.getLeft().getIndex())
                        .build())
                    .build())
                .build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    get.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, counter) -> counter.get(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            io.atomix.core.counter.impl.GetRequest.newBuilder().build())
            .thenApply(response -> GetResponse.newBuilder()
                .setHeaders(ResponseHeaders.newBuilder()
                    .addHeaders(ResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
                        .setIndex(response.getLeft().getIndex())
                        .build())
                    .build())
                .setValue(response.getRight().getValue())
                .build()));
  }

  @Override
  public void checkAndSet(CheckAndSetRequest request, StreamObserver<CheckAndSetResponse> responseObserver) {
    checkAndSet.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, counter) -> counter.checkAndSet(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            io.atomix.core.counter.impl.CheckAndSetRequest.newBuilder()
                .setExpect(request.getExpect())
                .setUpdate(request.getUpdate())
                .build())
            .thenApply(response -> CheckAndSetResponse.newBuilder()
                .setHeaders(ResponseHeaders.newBuilder()
                    .addHeaders(ResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
                        .setIndex(response.getLeft().getIndex())
                        .build())
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void increment(IncrementRequest request, StreamObserver<IncrementResponse> responseObserver) {
    increment.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, counter) -> counter.increment(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            io.atomix.core.counter.impl.IncrementRequest.newBuilder()
                .setDelta(request.getDelta())
                .build())
            .thenApply(response -> IncrementResponse.newBuilder()
                .setHeaders(ResponseHeaders.newBuilder()
                    .addHeaders(ResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
                        .setIndex(response.getLeft().getIndex())
                        .build())
                    .build())
                .setPreviousValue(response.getRight().getPreviousValue())
                .setNextValue(response.getRight().getNextValue())
                .build()));
  }

  @Override
  public void decrement(DecrementRequest request, StreamObserver<DecrementResponse> responseObserver) {
    decrement.executeBy(request, request.getId().getName(), responseObserver,
        (partitionId, header, counter) -> counter.decrement(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            io.atomix.core.counter.impl.DecrementRequest.newBuilder()
                .setDelta(request.getDelta())
                .build())
            .thenApply(response -> DecrementResponse.newBuilder()
                .setHeaders(ResponseHeaders.newBuilder()
                    .addHeaders(ResponseHeader.newBuilder()
                        .setPartitionId(partitionId.getPartition())
                        .setIndex(response.getLeft().getIndex())
                        .build())
                    .build())
                .setPreviousValue(response.getRight().getPreviousValue())
                .setNextValue(response.getRight().getNextValue())
                .build()));
  }

  private static final PrimitiveFactory.PrimitiveIdDescriptor<CounterId> COUNTER_ID_DESCRIPTOR = new PrimitiveFactory.PrimitiveIdDescriptor<CounterId>() {
    @Override
    public String getName(CounterId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(CounterId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(CounterId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(CounterId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(CounterId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(CounterId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(CounterId id) {
      return id.getLog();
    }
  };

  private static final RequestExecutor.RequestDescriptor<GetRequest, CounterId, RequestHeader> GET_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(GetRequest::getId, GetRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<SetRequest, CounterId, RequestHeader> SET_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(SetRequest::getId, SetRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<CheckAndSetRequest, CounterId, RequestHeader> CHECK_AND_SET_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(CheckAndSetRequest::getId, CheckAndSetRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<IncrementRequest, CounterId, RequestHeader> INCREMENT_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(IncrementRequest::getId, IncrementRequest::getHeaders);

  private static final RequestExecutor.RequestDescriptor<DecrementRequest, CounterId, RequestHeader> DECREMENT_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(DecrementRequest::getId, DecrementRequest::getHeaders);
}
