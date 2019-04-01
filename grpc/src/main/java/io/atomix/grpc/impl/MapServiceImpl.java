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
import io.atomix.core.Atomix;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.grpc.map.ClearRequest;
import io.atomix.grpc.map.ClearResponse;
import io.atomix.grpc.map.GetRequest;
import io.atomix.grpc.map.GetResponse;
import io.atomix.grpc.map.MapEvent;
import io.atomix.grpc.map.MapId;
import io.atomix.grpc.map.MapServiceGrpc;
import io.atomix.grpc.map.PutRequest;
import io.atomix.grpc.map.PutResponse;
import io.atomix.grpc.map.RemoveRequest;
import io.atomix.grpc.map.RemoveResponse;
import io.atomix.grpc.map.ReplaceRequest;
import io.atomix.grpc.map.ReplaceResponse;
import io.atomix.grpc.map.SizeRequest;
import io.atomix.grpc.map.SizeResponse;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
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
      return MultiPrimaryProtocol.builder(id.getMultiPrimary().getGroup())
          .build();
    } else if (id.hasLog()) {
      return DistributedLogProtocol.builder(id.getLog().getGroup())
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

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    run(request.getId(), map -> map.size().thenApply(size -> SizeResponse.newBuilder().setSize(size).build()), responseObserver);
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    run(request.getId(), map -> map.putAndGet(request.getKey(), request.getValue().toByteArray())
        .thenApply(versioned -> PutResponse.newBuilder()
            .setVersion(versioned.version())
            .build()), responseObserver);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    run(request.getId(), map -> map.get(request.getKey())
        .thenApply(versioned -> GetResponse.newBuilder()
            .setValue(versioned != null ? ByteString.copyFrom(versioned.value()) : ByteString.EMPTY)
            .setVersion(versioned != null ? versioned.version() : 0)
            .build()), responseObserver);
  }

  @Override
  public void replace(ReplaceRequest request, StreamObserver<ReplaceResponse> responseObserver) {
    run(request.getId(), map -> map.replace(request.getKey(), request.getVersion(), request.getValue().toByteArray())
        .thenApply(succeeded -> ReplaceResponse.newBuilder()
            .setSucceeded(succeeded)
            .build()), responseObserver);
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    if (request.getVersion() == 0) {
      run(request.getId(), map -> map.remove(request.getKey())
          .thenApply(succeeded -> RemoveResponse.newBuilder()
              .setSucceeded(true)
              .build()), responseObserver);
    } else {
      run(request.getId(), map -> map.remove(request.getKey(), request.getVersion())
          .thenApply(succeeded -> RemoveResponse.newBuilder()
              .setSucceeded(succeeded)
              .build()), responseObserver);
    }
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    run(request.getId(), map -> map.clear().thenApply(v -> ClearResponse.newBuilder().build()), responseObserver);
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
                  .setValue(ByteString.copyFrom(event.newValue().value()))
                  .setVersion(event.newValue().version())
                  .build());
              break;
            case UPDATE:
              responseObserver.onNext(MapEvent.newBuilder()
                  .setId(id)
                  .setType(MapEvent.Type.UPDATE)
                  .setKey(event.key())
                  .setValue(ByteString.copyFrom(event.newValue().value()))
                  .setVersion(event.newValue().version())
                  .build());
              break;
            case REMOVE:
              responseObserver.onNext(MapEvent.newBuilder()
                  .setId(id)
                  .setType(MapEvent.Type.REMOVE)
                  .setKey(event.key())
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
