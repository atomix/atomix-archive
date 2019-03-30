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
import io.atomix.core.multimap.AsyncAtomicMultimap;
import io.atomix.core.multimap.AtomicMultimapEventListener;
import io.atomix.grpc.multimap.ClearRequest;
import io.atomix.grpc.multimap.ClearResponse;
import io.atomix.grpc.multimap.GetRequest;
import io.atomix.grpc.multimap.GetResponse;
import io.atomix.grpc.multimap.MultimapEvent;
import io.atomix.grpc.multimap.MultimapId;
import io.atomix.grpc.multimap.MultimapServiceGrpc;
import io.atomix.grpc.multimap.PutRequest;
import io.atomix.grpc.multimap.PutResponse;
import io.atomix.grpc.multimap.RemoveRequest;
import io.atomix.grpc.multimap.RemoveResponse;
import io.atomix.grpc.multimap.SizeRequest;
import io.atomix.grpc.multimap.SizeResponse;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Multimap service implementation.
 */
public class MultimapServiceImpl extends MultimapServiceGrpc.MultimapServiceImplBase {
  private final Atomix atomix;

  public MultimapServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(MultimapId id) {
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

  private CompletableFuture<AsyncAtomicMultimap<String, byte[]>> getMultimap(MultimapId id) {
    return atomix.<String, byte[]>atomicMultimapBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(multimap -> multimap.async());
  }

  private <T> void run(MultimapId id, Function<AsyncAtomicMultimap<String, byte[]>, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getMultimap(id).whenComplete((multimap, getError) -> {
      if (getError == null) {
        function.apply(multimap).whenComplete((result, funcError) -> {
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
    run(request.getId(), map -> map.put(request.getKey(), request.getValue().toByteArray())
        .thenApply(succeeded -> PutResponse.newBuilder()
            .setSucceeded(succeeded)
            .build()), responseObserver);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    run(request.getId(), map -> map.get(request.getKey())
        .thenApply(versioned ->  GetResponse.newBuilder()
              .addAllValues(versioned.value().stream().map(ByteString::copyFrom).collect(Collectors.toSet()))
              .setVersion(versioned.version())
              .build()), responseObserver);
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      run(request.getId(), multimap -> multimap.removeAll(request.getKey())
          .thenApply(succeeded -> RemoveResponse.newBuilder()
              .setSucceeded(true)
              .build()), responseObserver);
    } else if (request.getValuesCount() == 1) {
      run(request.getId(), map -> map.remove(request.getKey(), request.getValues(0).toByteArray())
          .thenApply(succeeded -> RemoveResponse.newBuilder()
              .setSucceeded(true)
              .build()), responseObserver);
    } else {
      run(request.getId(), map -> map.removeAll(request.getKey(), request.getValuesList()
          .stream()
          .map(ByteString::toByteArray)
          .collect(Collectors.toSet()))
          .thenApply(succeeded -> RemoveResponse.newBuilder()
              .setSucceeded(true)
              .build()), responseObserver);
    }
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    run(request.getId(), map -> map.clear().thenApply(v -> ClearResponse.newBuilder().build()), responseObserver);
  }

  @Override
  public StreamObserver<MultimapId> listen(StreamObserver<MultimapEvent> responseObserver) {
    Map<MultimapId, AtomicMultimapEventListener<String, byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<MultimapId>() {
      @Override
      public void onNext(MultimapId id) {
        AtomicMultimapEventListener<String, byte[]> listener = event -> {
          switch (event.type()) {
            case INSERT:
              responseObserver.onNext(MultimapEvent.newBuilder()
                  .setId(id)
                  .setType(MultimapEvent.Type.INSERT)
                  .setKey(event.key())
                  .setValue(ByteString.copyFrom(event.newValue()))
                  .build());
              break;
            case REMOVE:
              responseObserver.onNext(MultimapEvent.newBuilder()
                  .setId(id)
                  .setType(MultimapEvent.Type.REMOVE)
                  .setKey(event.key())
                  .setValue(ByteString.copyFrom(event.oldValue()))
                  .build());
              break;
          }
        };
        listeners.put(id, listener);
        getMultimap(id).thenAccept(multimap -> multimap.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> getMultimap(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> getMultimap(id).thenAccept(map -> map.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
