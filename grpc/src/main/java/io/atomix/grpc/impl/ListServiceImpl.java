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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.list.AsyncDistributedList;
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
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * List service implementation.
 */
public class ListServiceImpl extends ListServiceGrpc.ListServiceImplBase {
  private final Atomix atomix;

  public ListServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(ListId id) {
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

  private CompletableFuture<AsyncDistributedList<byte[]>> getList(ListId id) {
    return atomix.<byte[]>listBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(list -> list.async());
  }

  private <T> void run(ListId id, Function<AsyncDistributedList<byte[]>, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getList(id).whenComplete((list, getError) -> {
      if (getError == null) {
        function.apply(list).whenComplete((result, funcError) -> {
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
  public void add(AddRequest request, StreamObserver<AddResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      responseObserver.onNext(AddResponse.newBuilder().setAdded(false).build());
      responseObserver.onCompleted();
    } else if (request.getValuesCount() == 1) {
      run(request.getId(), list -> list.add(request.getValues(0).toByteArray())
          .thenApply(added -> AddResponse.newBuilder()
              .setAdded(added)
              .build()), responseObserver);
    } else {
      run(request.getId(), list -> list.addAll(request.getValuesList()
          .stream()
          .map(ByteString::toByteArray)
          .collect(Collectors.toSet()))
          .thenApply(added -> AddResponse.newBuilder()
              .setAdded(added)
              .build()), responseObserver);
    }
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      responseObserver.onNext(RemoveResponse.newBuilder().setRemoved(false).build());
      responseObserver.onCompleted();
    } else if (request.getValuesCount() == 1) {
      run(request.getId(), list -> list.remove(request.getValues(0).toByteArray())
          .thenApply(removed -> RemoveResponse.newBuilder()
              .setRemoved(removed)
              .build()), responseObserver);
    } else {
      run(request.getId(), list -> list.removeAll(request.getValuesList()
          .stream()
          .map(ByteString::toByteArray)
          .collect(Collectors.toSet()))
          .thenApply(removed -> RemoveResponse.newBuilder()
              .setRemoved(removed)
              .build()), responseObserver);
    }
  }

  @Override
  public void contains(ContainsRequest request, StreamObserver<ContainsResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      responseObserver.onNext(ContainsResponse.newBuilder().setContains(false).build());
      responseObserver.onCompleted();
    } else if (request.getValuesCount() == 1) {
      run(request.getId(), list -> list.contains(request.getValues(0).toByteArray())
          .thenApply(contains -> ContainsResponse.newBuilder()
              .setContains(contains)
              .build()), responseObserver);
    } else {
      run(request.getId(), list -> list.containsAll(request.getValuesList()
          .stream()
          .map(ByteString::toByteArray)
          .collect(Collectors.toSet()))
          .thenApply(contains -> ContainsResponse.newBuilder()
              .setContains(contains)
              .build()), responseObserver);
    }
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    run(request.getId(), list -> list.get(request.getIndex())
        .thenApply(value -> GetResponse.newBuilder().setValue(ByteString.copyFrom(value)).build()), responseObserver);
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    run(request.getId(), list -> list.size().thenApply(size -> SizeResponse.newBuilder().setSize(size).build()), responseObserver);
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    run(request.getId(), list -> list.clear().thenApply(v -> ClearResponse.newBuilder().build()), responseObserver);
  }

  @Override
  public StreamObserver<ListId> listen(StreamObserver<ListEvent> responseObserver) {
    Map<ListId, CollectionEventListener<byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<ListId>() {
      @Override
      public void onNext(ListId id) {
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
        getList(id).thenAccept(set -> set.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> getList(id).thenAccept(list -> list.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> getList(id).thenAccept(list -> list.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
