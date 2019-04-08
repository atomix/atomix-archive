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
import io.atomix.core.multimap.AsyncAtomicMultimap;
import io.atomix.core.multimap.AtomicMultimap;
import io.atomix.core.multimap.AtomicMultimapEventListener;
import io.atomix.core.multimap.AtomicMultimapType;
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
import io.grpc.stub.StreamObserver;

/**
 * Multimap service implementation.
 */
public class MultimapServiceImpl extends MultimapServiceGrpc.MultimapServiceImplBase {
  private final PrimitiveExecutor<AtomicMultimap<String, byte[]>, AsyncAtomicMultimap<String, byte[]>> executor;

  public MultimapServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, AtomicMultimapType.instance(), AtomicMultimap::async);
  }

  @Override
  public void size(SizeRequest request, StreamObserver<SizeResponse> responseObserver) {
    executor.execute(request, SizeResponse::getDefaultInstance, responseObserver,
        multimap -> multimap.size().thenApply(size -> SizeResponse.newBuilder().setSize(size).build()));
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    executor.execute(request, PutResponse::getDefaultInstance, responseObserver,
        multimap -> multimap.put(request.getKey(), request.getValue().toByteArray())
            .thenApply(succeeded -> PutResponse.newBuilder()
                .setSucceeded(succeeded)
                .build()));
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    executor.execute(request, GetResponse::getDefaultInstance, responseObserver,
        multimap -> multimap.get(request.getKey())
            .thenApply(versioned ->  GetResponse.newBuilder()
                .addAllValues(versioned.value().stream().map(ByteString::copyFrom).collect(Collectors.toSet()))
                .setVersion(versioned.version())
                .build()));
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    if (request.getValuesCount() == 0) {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          multimap -> multimap.removeAll(request.getKey())
              .thenApply(succeeded -> RemoveResponse.newBuilder()
                  .setSucceeded(true)
                  .build()));
    } else if (request.getValuesCount() == 1) {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          multimap -> multimap.remove(request.getKey(), request.getValues(0).toByteArray())
              .thenApply(succeeded -> RemoveResponse.newBuilder()
                  .setSucceeded(true)
                  .build()));
    } else {
      executor.execute(request, RemoveResponse::getDefaultInstance, responseObserver,
          multimap -> multimap.removeAll(request.getKey(), request.getValuesList()
              .stream()
              .map(ByteString::toByteArray)
              .collect(Collectors.toSet()))
              .thenApply(succeeded -> RemoveResponse.newBuilder()
                  .setSucceeded(true)
                  .build()));
    }
  }

  @Override
  public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
    executor.execute(request, ClearResponse::getDefaultInstance, responseObserver,
        multimap -> multimap.clear().thenApply(v -> ClearResponse.newBuilder().build()));
  }

  @Override
  public StreamObserver<MultimapId> listen(StreamObserver<MultimapEvent> responseObserver) {
    Map<MultimapId, AtomicMultimapEventListener<String, byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<MultimapId>() {
      @Override
      public void onNext(MultimapId id) {
        if (!executor.isValidId(id, MultimapEvent::getDefaultInstance, responseObserver)) {
          return;
        }

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
        executor.getPrimitive(id).thenAccept(multimap -> multimap.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(multimap -> multimap.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> executor.getPrimitive(id).thenAccept(multimap -> multimap.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
