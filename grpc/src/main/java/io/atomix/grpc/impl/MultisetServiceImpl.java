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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.multiset.AsyncDistributedMultiset;
import io.atomix.core.multiset.DistributedMultiset;
import io.atomix.core.multiset.DistributedMultisetType;
import io.atomix.grpc.multiset.AddRequest;
import io.atomix.grpc.multiset.AddResponse;
import io.atomix.grpc.multiset.ClearRequest;
import io.atomix.grpc.multiset.ClearResponse;
import io.atomix.grpc.multiset.ContainsRequest;
import io.atomix.grpc.multiset.ContainsResponse;
import io.atomix.grpc.multiset.CountRequest;
import io.atomix.grpc.multiset.CountResponse;
import io.atomix.grpc.multiset.MultisetEvent;
import io.atomix.grpc.multiset.MultisetId;
import io.atomix.grpc.multiset.MultisetServiceGrpc;
import io.atomix.grpc.multiset.RemoveRequest;
import io.atomix.grpc.multiset.RemoveResponse;
import io.atomix.grpc.multiset.SetRequest;
import io.atomix.grpc.multiset.SetResponse;
import io.atomix.grpc.multiset.SizeRequest;
import io.atomix.grpc.multiset.SizeResponse;
import io.grpc.stub.StreamObserver;

/**
 * Multiset service implementation.
 */
public class MultisetServiceImpl extends MultisetServiceGrpc.MultisetServiceImplBase {
  private final PrimitiveExecutor<DistributedMultiset<byte[]>, AsyncDistributedMultiset<byte[]>> executor;

  public MultisetServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, DistributedMultisetType.instance(), DistributedMultiset::async);
  }

  @Override
  public void add(AddRequest request, StreamObserver<AddResponse> responseObserver) {
    executor.execute(request, AddResponse::getDefaultInstance, responseObserver,
        multiset -> multiset.add(
            request.getValue().toByteArray(), request.getOccurrences() > 0 ? request.getOccurrences() : 1)
            .thenApply(count -> AddResponse.newBuilder().setCount(count).build()));
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    executor.execute(request, SetResponse::getDefaultInstance, responseObserver,
        multiset -> multiset.setCount(
            request.getValue().toByteArray(), request.getCount())
            .thenApply(count -> SetResponse.newBuilder().setCount(count).build()));
  }

  @Override
  public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
    executor.execute(request, CountResponse::getDefaultInstance, responseObserver,
        multiset -> multiset.count(request.getValue().toByteArray())
            .thenApply(count -> CountResponse.newBuilder().setCount(count).build()));
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    if (request.getOccurrences() == 0) {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          multiset -> multiset.remove(request.getValue().toByteArray())
              .thenApply(count -> RemoveResponse.newBuilder().build()));
    } else {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          multiset -> multiset.remove(request.getValue().toByteArray(), request.getOccurrences())
              .thenApply(count -> RemoveResponse.newBuilder()
                  .setCount(count)
                  .build()));
    }
  }

  @Override
  public void contains(ContainsRequest request, StreamObserver<ContainsResponse> responseObserver) {
    executor.execute(request, ContainsResponse::getDefaultInstance, responseObserver,
        multiset -> multiset.contains(request.getValue().toByteArray())
            .thenApply(contains -> ContainsResponse.newBuilder()
                .setContains(contains)
                .build()));
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    executor.execute(request, SizeResponse::getDefaultInstance, responseObserver,
        multiset -> multiset.size().thenApply(size -> SizeResponse.newBuilder().setSize(size).build()));
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    executor.execute(request, ClearResponse::getDefaultInstance, responseObserver,
        multiset -> multiset.clear().thenApply(v -> ClearResponse.newBuilder().build()));
  }

  @Override
  public StreamObserver<MultisetId> listen(StreamObserver<MultisetEvent> responseObserver) {
    Map<MultisetId, CollectionEventListener<byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<MultisetId>() {
      @Override
      public void onNext(MultisetId id) {
        if (!executor.isValidId(id, MultisetEvent::getDefaultInstance, responseObserver)) {
          return;
        }

        CollectionEventListener<byte[]> listener = event -> {
          switch (event.type()) {
            case ADD:
              responseObserver.onNext(MultisetEvent.newBuilder()
                  .setId(id)
                  .setType(MultisetEvent.Type.ADDED)
                  .setValue(ByteString.copyFrom(event.element()))
                  .build());
              break;
            case REMOVE:
              responseObserver.onNext(MultisetEvent.newBuilder()
                  .setId(id)
                  .setType(MultisetEvent.Type.REMOVED)
                  .setValue(ByteString.copyFrom(event.element()))
                  .build());
              break;
          }
        };
        listeners.put(id, listener);
        executor.getPrimitive(id).thenAccept(multiset -> multiset.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(multiset -> multiset.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(multiset -> multiset.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
