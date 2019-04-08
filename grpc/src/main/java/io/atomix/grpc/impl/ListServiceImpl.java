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
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.list.AsyncDistributedList;
import io.atomix.core.list.DistributedList;
import io.atomix.core.list.DistributedListType;
import io.atomix.grpc.list.AddRequest;
import io.atomix.grpc.list.AddResponse;
import io.atomix.grpc.list.ClearRequest;
import io.atomix.grpc.list.ClearResponse;
import io.atomix.grpc.list.ContainsRequest;
import io.atomix.grpc.list.ContainsResponse;
import io.atomix.grpc.list.GetRequest;
import io.atomix.grpc.list.GetResponse;
import io.atomix.grpc.list.ListEvent;
import io.atomix.grpc.list.ListId;
import io.atomix.grpc.list.ListServiceGrpc;
import io.atomix.grpc.list.RemoveRequest;
import io.atomix.grpc.list.RemoveResponse;
import io.atomix.grpc.list.SizeRequest;
import io.atomix.grpc.list.SizeResponse;
import io.grpc.stub.StreamObserver;

/**
 * List service implementation.
 */
public class ListServiceImpl extends ListServiceGrpc.ListServiceImplBase {
  private final PrimitiveExecutor<DistributedList<byte[]>, AsyncDistributedList<byte[]>> executor;

  public ListServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, DistributedListType.instance(), DistributedList::async);
  }

  @Override
  public void add(AddRequest request, StreamObserver<AddResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      responseObserver.onNext(AddResponse.newBuilder().setAdded(false).build());
      responseObserver.onCompleted();
    } else if (request.getValuesCount() == 1) {
      executor.execute(request, AddResponse::getDefaultInstance, responseObserver,
          list -> list.add(request.getValues(0).toByteArray())
              .thenApply(added -> AddResponse.newBuilder()
                  .setAdded(added)
                  .build()));
    } else {
      executor.execute(request, AddResponse::getDefaultInstance, responseObserver,
          list -> list.addAll(request.getValuesList()
              .stream()
              .map(ByteString::toByteArray)
              .collect(Collectors.toSet()))
              .thenApply(added -> AddResponse.newBuilder()
                  .setAdded(added)
                  .build()));
    }
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      responseObserver.onNext(RemoveResponse.newBuilder().setRemoved(false).build());
      responseObserver.onCompleted();
    } else if (request.getValuesCount() == 1) {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          list -> list.remove(request.getValues(0).toByteArray())
              .thenApply(removed -> RemoveResponse.newBuilder()
                  .setRemoved(removed)
                  .build()));
    } else {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          list -> list.removeAll(request.getValuesList()
              .stream()
              .map(ByteString::toByteArray)
              .collect(Collectors.toSet()))
              .thenApply(removed -> RemoveResponse.newBuilder()
                  .setRemoved(removed)
                  .build()));
    }
  }

  @Override
  public void contains(ContainsRequest request, StreamObserver<ContainsResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      responseObserver.onNext(ContainsResponse.newBuilder().setContains(false).build());
      responseObserver.onCompleted();
    } else if (request.getValuesCount() == 1) {
      executor.execute(request, ContainsResponse::getDefaultInstance, responseObserver,
          list -> list.contains(request.getValues(0).toByteArray())
              .thenApply(contains -> ContainsResponse.newBuilder()
                  .setContains(contains)
                  .build()));
    } else {
      executor.execute(request, ContainsResponse::getDefaultInstance, responseObserver,
          list -> list.containsAll(request.getValuesList()
              .stream()
              .map(ByteString::toByteArray)
              .collect(Collectors.toSet()))
              .thenApply(contains -> ContainsResponse.newBuilder()
                  .setContains(contains)
                  .build()));
    }
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    executor.execute(request, GetResponse::getDefaultInstance, responseObserver,
        list -> list.get(request.getIndex())
            .thenApply(value -> GetResponse.newBuilder()
                .setValue(ByteString.copyFrom(value))
                .build()));
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    executor.execute(request, SizeResponse::getDefaultInstance, responseObserver,
        list -> list.size()
            .thenApply(size -> SizeResponse.newBuilder()
                .setSize(size)
                .build()));
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    executor.execute(request, ClearResponse::getDefaultInstance, responseObserver,
        list -> list.clear().thenApply(v -> ClearResponse.newBuilder().build()));
  }

  @Override
  public StreamObserver<ListId> listen(StreamObserver<ListEvent> responseObserver) {
    Map<ListId, CollectionEventListener<byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<ListId>() {
      @Override
      public void onNext(ListId id) {
        if (executor.isValidId(id, ListEvent::getDefaultInstance, responseObserver)) {
          CollectionEventListener<byte[]> listener = event -> {
            switch (event.type()) {
              case ADD:
                responseObserver.onNext(ListEvent.newBuilder()
                    .setId(id)
                    .setType(ListEvent.Type.ADDED)
                    .setValue(ByteString.copyFrom(event.element()))
                    .build());
                break;
              case REMOVE:
                responseObserver.onNext(ListEvent.newBuilder()
                    .setId(id)
                    .setType(ListEvent.Type.REMOVED)
                    .setValue(ByteString.copyFrom(event.element()))
                    .build());
                break;
            }
          };
          listeners.put(id, listener);
          executor.getPrimitive(id).thenAccept(set -> set.addListener(listener));
        }
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(list -> list.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(list -> list.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
