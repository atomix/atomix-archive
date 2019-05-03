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

import java.util.Collection;
import java.util.stream.Collectors;

import io.atomix.core.Atomix;
import io.atomix.grpc.primitives.GetPrimitivesRequest;
import io.atomix.grpc.primitives.GetPrimitivesResponse;
import io.atomix.grpc.primitives.PrimitiveInfo;
import io.atomix.grpc.primitives.PrimitivesServiceGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Distributed primitives service implementation.
 */
public class PrimitivesServiceImpl extends PrimitivesServiceGrpc.PrimitivesServiceImplBase {
  private final Atomix atomix;

  public PrimitivesServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  @Override
  public void getPrimitives(GetPrimitivesRequest request, StreamObserver<GetPrimitivesResponse> responseObserver) {
    Collection<io.atomix.primitive.PrimitiveInfo> primitives;
    if (request.getType().equals("")) {
      primitives = atomix.getPrimitives();
    } else {
      primitives = atomix.getPrimitives(atomix.getPrimitiveType(request.getType()));
    }
    responseObserver.onNext(GetPrimitivesResponse.newBuilder()
        .addAllInfo(primitives.stream()
            .map(info -> PrimitiveInfo.newBuilder()
                .setType(info.type().name())
                .setName(info.name())
                .build())
            .collect(Collectors.toList()))
        .build());
    responseObserver.onCompleted();
  }
}
