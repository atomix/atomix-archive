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

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.atomix.core.Atomix;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.grpc.collection.Contains;
import io.atomix.grpc.collection.IsEmpty;
import io.atomix.grpc.collection.Size;
import io.atomix.grpc.map.MapEntryRequest;
import io.atomix.grpc.map.MapEvent;
import io.atomix.grpc.map.MapId;
import io.atomix.grpc.map.MapKeyRequest;
import io.atomix.grpc.map.MapServiceGrpc;
import io.atomix.grpc.map.MapValueRequest;
import io.atomix.grpc.map.MapValueResponse;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.utils.time.Versioned;
import io.grpc.stub.StreamObserver;

/**
 * Map service implementation.
 */
public class MapServiceImpl extends MapServiceGrpc.MapServiceImplBase {
  private final Atomix atomix;

  public MapServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(MapId id) {
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

  private CompletableFuture<AsyncAtomicMap<String, byte[]>> getMap(MapId id) {
    return atomix.<String, byte[]>atomicMapBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(map -> map.async());
  }

  private <T> void run(MapId id, Function<AsyncAtomicMap<String, byte[]>, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getMap(id).whenComplete((map, getError) -> {
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

  private MapValueResponse toValueResponse(Versioned<byte[]> value) {
    return value == null ? null : MapValueResponse.newBuilder()
        .setValue(ByteString.copyFrom(value.value()))
        .setVersion(value.version())
        .build();
  }

  @Override
  public void size(MapId request, StreamObserver<Size> responseObserver) {
    run(request, map -> map.size().thenApply(this::toSize), responseObserver);
  }

  @Override
  public void isEmpty(MapId request, StreamObserver<IsEmpty> responseObserver) {
    run(request, map -> map.isEmpty().thenApply(this::toIsEmpty), responseObserver);
  }

  @Override
  public void containsKey(MapKeyRequest request, StreamObserver<Contains> responseObserver) {
    run(request.getId(), map -> map.containsKey(request.getKey()).thenApply(this::toContains), responseObserver);
  }

  @Override
  public void containsValue(MapValueRequest request, StreamObserver<Contains> responseObserver) {
    run(request.getId(), map -> map.containsValue(request.getValue().toByteArray()).thenApply(this::toContains), responseObserver);
  }

  @Override
  public void put(MapEntryRequest request, StreamObserver<MapValueResponse> responseObserver) {
    run(request.getId(), map -> map.put(request.getKey(), request.getValue().toByteArray()).thenApply(this::toValueResponse), responseObserver);
  }

  @Override
  public void get(MapKeyRequest request, StreamObserver<MapValueResponse> responseObserver) {
    run(request.getId(), map -> map.get(request.getKey()).thenApply(this::toValueResponse), responseObserver);
  }

  @Override
  public void replace(MapEntryRequest request, StreamObserver<MapValueResponse> responseObserver) {
    if (request.getVersion() > 0) {
      run(
          request.getId(),
          map -> map.replace(request.getKey(), request.getVersion(), request.getValue().toByteArray())
              .thenCompose(v -> map.get(request.getKey()))
              .thenApply(this::toValueResponse),
          responseObserver);
    } else {
      run(
          request.getId(),
          map -> map.replace(request.getKey(), request.getValue().toByteArray())
              .thenApply(this::toValueResponse),
          responseObserver);
    }
  }

  @Override
  public void remove(MapKeyRequest request, StreamObserver<MapValueResponse> responseObserver) {
    run(request.getId(), map -> map.remove(request.getKey()).thenApply(this::toValueResponse), responseObserver);
  }

  @Override
  public void clear(MapId request, StreamObserver<Empty> responseObserver) {
    run(request, map -> map.clear().thenApply(v -> Empty.newBuilder().build()), responseObserver);
  }

  @Override
  public StreamObserver<MapId> listen(StreamObserver<MapEvent> responseObserver) {
    Map<MapId, AtomicMapEventListener<String, byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<MapId>() {
      @Override
      public void onNext(MapId id) {
        AtomicMapEventListener<String, byte[]> listener = event -> {
          switch (event.type()) {
            case INSERT:
              responseObserver.onNext(MapEvent.newBuilder()
                  .setId(id)
                  .setType(MapEvent.Type.INSERT)
                  .setKey(event.key())
                  .setNewValue(toValueResponse(event.newValue()))
                  .build());
              break;
            case UPDATE:
              responseObserver.onNext(MapEvent.newBuilder()
                  .setId(id)
                  .setType(MapEvent.Type.UPDATE)
                  .setKey(event.key())
                  .setOldValue(toValueResponse(event.oldValue()))
                  .setNewValue(toValueResponse(event.newValue()))
                  .build());
              break;
            case REMOVE:
              responseObserver.onNext(MapEvent.newBuilder()
                  .setId(id)
                  .setType(MapEvent.Type.REMOVE)
                  .setKey(event.key())
                  .setOldValue(toValueResponse(event.oldValue()))
                  .build());
              break;
          }
        };
        listeners.put(id, listener);
        getMap(id).thenAccept(map -> map.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> getMap(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> getMap(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
