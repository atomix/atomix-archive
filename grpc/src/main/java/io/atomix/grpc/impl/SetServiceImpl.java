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
import io.atomix.core.set.AsyncDistributedSet;
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
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Set service implementation.
 */
public class SetServiceImpl extends SetServiceGrpc.SetServiceImplBase {
  private final Atomix atomix;

  public SetServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(SetId id) {
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

  private CompletableFuture<AsyncDistributedSet<byte[]>> getSet(SetId id) {
    return atomix.<byte[]>setBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(set -> set.async());
  }

  private <T> void run(SetId id, Function<AsyncDistributedSet<byte[]>, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getSet(id).whenComplete((set, getError) -> {
      if (getError == null) {
        function.apply(set).whenComplete((result, funcError) -> {
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
      run(request.getId(), set -> set.add(request.getValues(0).toByteArray())
          .thenApply(added -> AddResponse.newBuilder()
              .setAdded(added)
              .build()), responseObserver);
    } else {
      run(request.getId(), set -> set.addAll(request.getValuesList()
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
      run(request.getId(), set -> set.remove(request.getValues(0).toByteArray())
          .thenApply(removed -> RemoveResponse.newBuilder()
              .setRemoved(removed)
              .build()), responseObserver);
    } else {
      run(request.getId(), set -> set.removeAll(request.getValuesList()
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
      run(request.getId(), set -> set.contains(request.getValues(0).toByteArray())
          .thenApply(contains -> ContainsResponse.newBuilder()
              .setContains(contains)
              .build()), responseObserver);
    } else {
      run(request.getId(), set -> set.containsAll(request.getValuesList()
          .stream()
          .map(ByteString::toByteArray)
          .collect(Collectors.toSet()))
          .thenApply(contains -> ContainsResponse.newBuilder()
              .setContains(contains)
              .build()), responseObserver);
    }
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    run(request.getId(), set -> set.size().thenApply(size -> SizeResponse.newBuilder().setSize(size).build()), responseObserver);
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    run(request.getId(), set -> set.clear().thenApply(v -> ClearResponse.newBuilder().build()), responseObserver);
  }

  @Override
  public StreamObserver<SetId> listen(StreamObserver<SetEvent> responseObserver) {
    Map<SetId, CollectionEventListener<byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<SetId>() {
      @Override
      public void onNext(SetId id) {
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
        getSet(id).thenAccept(set -> set.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> getSet(id).thenAccept(set -> set.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> getSet(id).thenAccept(set -> set.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
