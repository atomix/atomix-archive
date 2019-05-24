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
package io.atomix.server.counter;

import java.util.Collections;

import io.atomix.api.counter.CounterId;
import io.atomix.api.headers.RequestHeader;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.protocol.DistributedLogProtocol;
import io.atomix.api.protocol.MultiPrimaryProtocol;
import io.atomix.api.protocol.MultiRaftProtocol;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.service.impl.DefaultServiceClient;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.server.impl.PrimitiveFactory;
import io.atomix.server.impl.PrimitiveIdDescriptor;
import io.atomix.server.impl.RequestExecutor;
import io.grpc.stub.StreamObserver;

/**
 * Counter service implementation.
 */
public class CounterServiceImpl extends io.atomix.api.counter.CounterServiceGrpc.CounterServiceImplBase {
  private final PrimitiveFactory<CounterProxy, CounterId> primitiveFactory;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, io.atomix.api.counter.CreateRequest, io.atomix.api.counter.CreateResponse> create;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, io.atomix.api.counter.DeleteRequest, io.atomix.api.counter.DeleteResponse> delete;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, io.atomix.api.counter.SetRequest, io.atomix.api.counter.SetResponse> set;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, io.atomix.api.counter.GetRequest, io.atomix.api.counter.GetResponse> get;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, io.atomix.api.counter.CheckAndSetRequest, io.atomix.api.counter.CheckAndSetResponse> checkAndSet;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, io.atomix.api.counter.IncrementRequest, io.atomix.api.counter.IncrementResponse> increment;
  private final RequestExecutor<CounterProxy, CounterId, RequestHeader, io.atomix.api.counter.DecrementRequest, io.atomix.api.counter.DecrementResponse> decrement;

  public CounterServiceImpl(PartitionService partitionService) {
    this.primitiveFactory = new PrimitiveFactory<>(
        partitionService,
        CounterService.TYPE,
        (id, client) -> new CounterProxy(new DefaultServiceClient(id, client)),
        COUNTER_ID_DESCRIPTOR);
    this.create = new RequestExecutor<>(primitiveFactory, CREATE_DESCRIPTOR, io.atomix.api.counter.CreateResponse::getDefaultInstance);
    this.delete = new RequestExecutor<>(primitiveFactory, DELETE_DESCRIPTOR, io.atomix.api.counter.DeleteResponse::getDefaultInstance);
    this.set = new RequestExecutor<>(primitiveFactory, SET_DESCRIPTOR, io.atomix.api.counter.SetResponse::getDefaultInstance);
    this.get = new RequestExecutor<>(primitiveFactory, GET_DESCRIPTOR, io.atomix.api.counter.GetResponse::getDefaultInstance);
    this.checkAndSet = new RequestExecutor<>(primitiveFactory, CHECK_AND_SET_DESCRIPTOR, io.atomix.api.counter.CheckAndSetResponse::getDefaultInstance);
    this.increment = new RequestExecutor<>(primitiveFactory, INCREMENT_DESCRIPTOR, io.atomix.api.counter.IncrementResponse::getDefaultInstance);
    this.decrement = new RequestExecutor<>(primitiveFactory, DECREMENT_DESCRIPTOR, io.atomix.api.counter.DecrementResponse::getDefaultInstance);
  }

  @Override
  public void create(io.atomix.api.counter.CreateRequest request, StreamObserver<io.atomix.api.counter.CreateResponse> responseObserver) {
    create.createBy(request, request.getId().getName(), responseObserver,
        (partitionId, counter) -> counter.create()
            .thenApply(v -> io.atomix.api.counter.CreateResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setPartitionId(partitionId.getPartition())
                    .build())
                .build()));
  }

  @Override
  public void delete(io.atomix.api.counter.DeleteRequest request, StreamObserver<io.atomix.api.counter.DeleteResponse> responseObserver) {
    delete.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, counter) -> counter.delete()
            .thenApply(v -> io.atomix.api.counter.DeleteResponse.newBuilder().build()));
  }

  @Override
  public void set(io.atomix.api.counter.SetRequest request, StreamObserver<io.atomix.api.counter.SetResponse> responseObserver) {
    set.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, counter) -> counter.set(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            SetRequest.newBuilder()
                .setValue(request.getValue())
                .build())
            .thenApply(response -> io.atomix.api.counter.SetResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .build()));
  }

  @Override
  public void get(io.atomix.api.counter.GetRequest request, StreamObserver<io.atomix.api.counter.GetResponse> responseObserver) {
    get.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, counter) -> counter.get(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            GetRequest.newBuilder().build())
            .thenApply(response -> io.atomix.api.counter.GetResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setValue(response.getRight().getValue())
                .build()));
  }

  @Override
  public void checkAndSet(io.atomix.api.counter.CheckAndSetRequest request, StreamObserver<io.atomix.api.counter.CheckAndSetResponse> responseObserver) {
    checkAndSet.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, counter) -> counter.checkAndSet(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            CheckAndSetRequest.newBuilder()
                .setExpect(request.getExpect())
                .setUpdate(request.getUpdate())
                .build())
            .thenApply(response -> io.atomix.api.counter.CheckAndSetResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setSucceeded(response.getRight().getSucceeded())
                .build()));
  }

  @Override
  public void increment(io.atomix.api.counter.IncrementRequest request, StreamObserver<io.atomix.api.counter.IncrementResponse> responseObserver) {
    increment.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, counter) -> counter.increment(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            IncrementRequest.newBuilder()
                .setDelta(request.getDelta())
                .build())
            .thenApply(response -> io.atomix.api.counter.IncrementResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setPreviousValue(response.getRight().getPreviousValue())
                .setNextValue(response.getRight().getNextValue())
                .build()));
  }

  @Override
  public void decrement(io.atomix.api.counter.DecrementRequest request, StreamObserver<io.atomix.api.counter.DecrementResponse> responseObserver) {
    decrement.executeBy(request, request.getHeader(), responseObserver,
        (partitionId, header, counter) -> counter.decrement(
            RequestContext.newBuilder()
                .setIndex(header.getIndex())
                .build(),
            DecrementRequest.newBuilder()
                .setDelta(request.getDelta())
                .build())
            .thenApply(response -> io.atomix.api.counter.DecrementResponse.newBuilder()
                .setHeader(ResponseHeader.newBuilder()
                    .setPartitionId(partitionId.getPartition())
                    .setIndex(response.getLeft().getIndex())
                    .build())
                .setPreviousValue(response.getRight().getPreviousValue())
                .setNextValue(response.getRight().getNextValue())
                .build()));
  }

  private static final PrimitiveIdDescriptor<CounterId> COUNTER_ID_DESCRIPTOR = new PrimitiveIdDescriptor<CounterId>() {
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

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.counter.CreateRequest, CounterId, RequestHeader> CREATE_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(io.atomix.api.counter.CreateRequest::getId, request -> Collections.emptyList());

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.counter.DeleteRequest, CounterId, RequestHeader> DELETE_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(io.atomix.api.counter.DeleteRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.counter.GetRequest, CounterId, RequestHeader> GET_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(io.atomix.api.counter.GetRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.counter.SetRequest, CounterId, RequestHeader> SET_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(io.atomix.api.counter.SetRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.counter.CheckAndSetRequest, CounterId, RequestHeader> CHECK_AND_SET_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(io.atomix.api.counter.CheckAndSetRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.counter.IncrementRequest, CounterId, RequestHeader> INCREMENT_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(io.atomix.api.counter.IncrementRequest::getId, request -> Collections.singletonList(request.getHeader()));

  private static final RequestExecutor.RequestDescriptor<io.atomix.api.counter.DecrementRequest, CounterId, RequestHeader> DECREMENT_DESCRIPTOR =
      new RequestExecutor.BasicDescriptor<>(io.atomix.api.counter.DecrementRequest::getId, request -> Collections.singletonList(request.getHeader()));
}
