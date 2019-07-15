/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.node.primitive.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import io.atomix.api.primitive.GetPrimitivesRequest;
import io.atomix.api.primitive.GetPrimitivesResponse;
import io.atomix.api.primitive.Name;
import io.atomix.api.primitive.PrimitiveInfo;
import io.atomix.api.primitive.PrimitiveServiceGrpc;
import io.atomix.node.service.client.ClientFactory;
import io.atomix.node.service.client.MetadataClient;
import io.atomix.node.service.protocol.ServiceId;
import io.grpc.stub.StreamObserver;

/**
 * Primitive service implementation.
 */
public class PrimitiveService extends PrimitiveServiceGrpc.PrimitiveServiceImplBase {
    private final MetadataClient client;

    public PrimitiveService(ClientFactory factory) {
        this.client = factory.newMetadataClient();
    }

    @Override
    public void getPrimitives(GetPrimitivesRequest request, StreamObserver<GetPrimitivesResponse> responseObserver) {
        String type = request.getType();
        CompletableFuture<Collection<ServiceId>> future;
        if (Strings.isNullOrEmpty(type)) {
            future = client.getServices();
        } else {
            future = client.getServices(type);
        }
        future.whenComplete((services, error) -> {
            if (error == null) {
                responseObserver.onNext(GetPrimitivesResponse.newBuilder()
                    .addAllPrimitives(services.stream()
                        .map(service -> PrimitiveInfo.newBuilder()
                            .setType(service.getType())
                            .setName(Name.newBuilder()
                                .setName(service.getName())
                                .setNamespace(service.getNamespace())
                                .build())
                            .build())
                        .collect(Collectors.toList()))
                    .build());
            } else {
                responseObserver.onError(error);
            }
        });
    }
}
