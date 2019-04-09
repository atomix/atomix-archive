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
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.DistributedSetType;
import io.atomix.grpc.set.AddRequest;
import io.atomix.grpc.set.AddResponse;
import io.atomix.grpc.set.ClearRequest;
import io.atomix.grpc.set.ClearResponse;
import io.atomix.grpc.set.ContainsRequest;
import io.atomix.grpc.set.ContainsResponse;
import io.atomix.grpc.set.RemoveRequest;
import io.atomix.grpc.set.RemoveResponse;
import io.atomix.grpc.set.SetEvent;
import io.atomix.grpc.set.SetId;
import io.atomix.grpc.set.SetServiceGrpc;
import io.atomix.grpc.set.SizeRequest;
import io.atomix.grpc.set.SizeResponse;
import io.grpc.stub.StreamObserver;

/**
 * Set service implementation.
 */
public class SetServiceImpl extends SetServiceGrpc.SetServiceImplBase {
  private final PrimitiveExecutor<DistributedSet<byte[]>, AsyncDistributedSet<byte[]>> executor;

  public SetServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, DistributedSetType.instance(), DistributedSet::async);
  }

  @Override
  public void add(AddRequest request, StreamObserver<AddResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      responseObserver.onNext(AddResponse.newBuilder().setAdded(false).build());
      responseObserver.onCompleted();
    } else if (request.getValuesCount() == 1) {
      executor.execute(request, AddResponse::getDefaultInstance, responseObserver,
          set -> set.add(request.getValues(0).toByteArray())
          .thenApply(added -> AddResponse.newBuilder()
              .setAdded(added)
              .build()));
    } else {
      executor.execute(request, AddResponse::getDefaultInstance, responseObserver,
          set -> set.addAll(request.getValuesList()
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
          set -> set.remove(request.getValues(0).toByteArray())
          .thenApply(removed -> RemoveResponse.newBuilder()
              .setRemoved(removed)
              .build()));
    } else {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          set -> set.removeAll(request.getValuesList()
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
          set -> set.contains(request.getValues(0).toByteArray())
          .thenApply(contains -> ContainsResponse.newBuilder()
              .setContains(contains)
              .build()));
    } else {
      executor.execute(request, ContainsResponse::getDefaultInstance, responseObserver,
          set -> set.containsAll(request.getValuesList()
          .stream()
          .map(ByteString::toByteArray)
          .collect(Collectors.toSet()))
          .thenApply(contains -> ContainsResponse.newBuilder()
              .setContains(contains)
              .build()));
    }
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    executor.execute(request, SizeResponse::getDefaultInstance, responseObserver,
        set -> set.size().thenApply(size -> SizeResponse.newBuilder().setSize(size).build()));
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    executor.execute(request, ClearResponse::getDefaultInstance, responseObserver,
        set -> set.clear().thenApply(v -> ClearResponse.newBuilder().build()));
  }

  @Override
  public StreamObserver<SetId> listen(StreamObserver<SetEvent> responseObserver) {
    Map<SetId, CollectionEventListener<byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<SetId>() {
      @Override
      public void onNext(SetId id) {
        if (!executor.isValidId(id, SetEvent::getDefaultInstance, responseObserver)) {
          return;
        }

        CollectionEventListener<byte[]> listener = event -> {
          switch (event.type()) {
            case ADD:
              responseObserver.onNext(SetEvent.newBuilder()
                  .setId(id)
                  .setType(SetEvent.Type.ADDED)
                  .setValue(ByteString.copyFrom(event.element()))
                  .build());
              break;
            case REMOVE:
              responseObserver.onNext(SetEvent.newBuilder()
                  .setId(id)
                  .setType(SetEvent.Type.REMOVED)
                  .setValue(ByteString.copyFrom(event.element()))
                  .build());
              break;
          }
        };
        listeners.put(id, listener);
        executor.getPrimitive(id).thenAccept(set -> set.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(set -> set.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(set -> set.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
