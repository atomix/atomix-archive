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
package io.atomix.node.protocol.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.atomix.node.protocol.ProtocolClient;
import io.atomix.node.service.client.MetadataClient;
import io.atomix.node.service.protocol.MetadataRequest;
import io.atomix.node.service.protocol.ServiceId;
import io.atomix.node.service.protocol.ServiceRequest;
import io.atomix.node.service.protocol.ServiceResponse;
import io.atomix.node.service.util.ByteArrayDecoder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default metadata client.
 */
public class DefaultMetadataClient implements MetadataClient {
    private final ProtocolClient client;

    public DefaultMetadataClient(ProtocolClient client) {
        this.client = checkNotNull(client);
    }

    @Override
    public CompletableFuture<Collection<ServiceId>> getServices() {
        return client.query(ServiceRequest.newBuilder()
            .setMetadata(MetadataRequest.newBuilder().build())
            .build()
            .toByteArray())
            .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
            .thenApply(response -> response.getMetadata().getServicesList());
    }

    @Override
    public CompletableFuture<Collection<ServiceId>> getServices(String type) {
        return client.query(ServiceRequest.newBuilder()
            .setMetadata(MetadataRequest.newBuilder()
                .setType(type)
                .build())
            .build()
            .toByteArray())
            .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
            .thenApply(response -> response.getMetadata().getServicesList());
    }
}
