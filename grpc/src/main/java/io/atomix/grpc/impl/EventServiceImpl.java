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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.grpc.event.EventServiceGrpc;
import io.atomix.grpc.event.PublishRequest;
import io.atomix.grpc.event.PublishResponse;
import io.atomix.grpc.event.SubscribeRequest;
import io.atomix.grpc.event.SubscribeResponse;
import io.grpc.stub.StreamObserver;

/**
 * gRPC event service implementation.
 */
public class EventServiceImpl extends EventServiceGrpc.EventServiceImplBase {
  private final Atomix atomix;

  public EventServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  @Override
  public StreamObserver<PublishRequest> publish(StreamObserver<PublishResponse> responseObserver) {
    return new StreamObserver<PublishRequest>() {
      @Override
      public void onNext(PublishRequest value) {
        atomix.getEventService().broadcast(value.getTopic(), value.getPayload().toByteArray());
      }

      @Override
      public void onError(Throwable t) {
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void subscribe(SubscribeRequest request, StreamObserver<SubscribeResponse> responseObserver) {
    atomix.getEventService().<byte[]>subscribe(request.getTopic(), bytes -> {
      responseObserver.onNext(SubscribeResponse.newBuilder()
          .setPayload(ByteString.copyFrom(bytes))
          .build());
    }, MoreExecutors.directExecutor());
  }
}
