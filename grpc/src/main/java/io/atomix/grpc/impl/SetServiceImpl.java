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
import com.google.protobuf.Empty;
import io.atomix.core.Atomix;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.grpc.collection.Added;
import io.atomix.grpc.collection.Contains;
import io.atomix.grpc.collection.IsEmpty;
import io.atomix.grpc.collection.Removed;
import io.atomix.grpc.collection.Size;
import io.atomix.grpc.set.SetEvent;
import io.atomix.grpc.set.SetId;
import io.atomix.grpc.set.SetServiceGrpc;
import io.atomix.grpc.set.SetValue;
import io.atomix.grpc.set.SetValues;
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
      return MultiPrimaryProtocol.builder(id.getRaft().getGroup())
          .build();
    } else if (id.hasLog()) {
      return DistributedLogProtocol.builder(id.getRaft().getGroup())
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
    getSet(id).whenComplete((map, getError) -> {
      if (getError == null) {
        function.apply(map).whenComplete((result, funcError) -> {
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

  private Added toAdded(boolean added) {
    return Added.newBuilder()
        .setAdded(added)
        .build();
  }

  private Removed toRemoved(boolean removed) {
    return Removed.newBuilder()
        .setRemoved(removed)
        .build();
  }

  private Contains toContains(boolean contains) {
    return Contains.newBuilder()
        .setContains(contains)
        .build();
  }

  private Size toSize(int size) {
    return Size.newBuilder()
        .setSize(size)
        .build();
  }

  private IsEmpty toIsEmpty(boolean isEmpty) {
    return IsEmpty.newBuilder()
        .setIsEmpty(isEmpty)
        .build();
  }

  @Override
  public void add(SetValue request, StreamObserver<Added> responseObserver) {
    run(request.getId(), set -> set.add(request.getValue().toByteArray()).thenApply(this::toAdded), responseObserver);
  }

  @Override
  public void addAll(SetValues request, StreamObserver<Added> responseObserver) {
    run(request.getId(), set -> set.addAll(request.getValuesList()
        .stream()
        .map(ByteString::toByteArray)
        .collect(Collectors.toSet()))
        .thenApply(this::toAdded), responseObserver);
  }

  @Override
  public void remove(SetValue request, StreamObserver<Removed> responseObserver) {
    run(request.getId(), set -> set.remove(request.getValue().toByteArray()).thenApply(this::toRemoved), responseObserver);
  }

  @Override
  public void removeAll(SetValues request, StreamObserver<Removed> responseObserver) {
    run(request.getId(), set -> set.removeAll(request.getValuesList()
        .stream()
        .map(ByteString::toByteArray)
        .collect(Collectors.toSet()))
        .thenApply(this::toRemoved), responseObserver);
  }

  @Override
  public void contains(SetValue request, StreamObserver<Contains> responseObserver) {
    run(request.getId(), set -> set.contains(request.getValue().toByteArray()).thenApply(this::toContains), responseObserver);
  }

  @Override
  public void containsAll(SetValues request, StreamObserver<Contains> responseObserver) {
    run(request.getId(), set -> set.containsAll(request.getValuesList()
        .stream()
        .map(ByteString::toByteArray)
        .collect(Collectors.toSet()))
        .thenApply(this::toContains), responseObserver);
  }

  @Override
  public void size(SetId request, StreamObserver<Size> responseObserver) {
    run(request, set -> set.size().thenApply(this::toSize), responseObserver);
  }

  @Override
  public void isEmpty(SetId request, StreamObserver<IsEmpty> responseObserver) {
    run(request, set -> set.isEmpty().thenApply(this::toIsEmpty), responseObserver);
  }

  @Override
  public void clear(SetId request, StreamObserver<Empty> responseObserver) {
    run(request, set -> set.clear().thenApply(v -> Empty.newBuilder().build()), responseObserver);
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
        listeners.forEach((id, listener) -> getSet(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> getSet(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
