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
package io.atomix.node.service.client;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Message;
import io.atomix.node.service.operation.OperationId;
import io.atomix.node.service.protocol.RequestContext;
import io.atomix.node.service.protocol.ResponseContext;
import io.atomix.node.service.protocol.StreamContext;
import io.atomix.node.service.util.ByteBufferDecoder;
import io.atomix.node.service.util.ByteStringEncoder;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Service client.
 */
public interface ServiceClient {

    /**
     * Returns the service name.
     *
     * @return the service name
     */
    String name();

    /**
     * Returns the service type.
     *
     * @return the service type
     */
    String type();

    /**
     * Executes an operation on the primitive.
     *
     * @param operation the operation to execute
     * @param context   the request context
     * @param request   the request
     * @param encoder   the request encoder
     * @param decoder   the response decoder
     * @param <T>       the request type
     * @param <U>       the response type
     * @return a future to be completed with the response
     */
    <T extends Message, U extends Message> CompletableFuture<Pair<ResponseContext, U>> execute(
        OperationId operation,
        RequestContext context,
        T request,
        ByteStringEncoder<T> encoder,
        ByteBufferDecoder<U> decoder);

    /**
     * Executes a streaming operation on the primitive.
     *
     * @param operation the operation to execute
     * @param context   the request context
     * @param request   the request
     * @param encoder   the request encoder
     * @param handler   the response handler
     * @param decoder   the response decoder
     * @param <T>       the request type
     * @param <U>       the response type
     * @return a future to be completed once the operation has been completed
     */
    <T extends Message, U extends Message> CompletableFuture<Void> execute(
        OperationId operation,
        RequestContext context,
        T request,
        ByteStringEncoder<T> encoder,
        StreamHandler<Pair<StreamContext, U>> handler,
        ByteBufferDecoder<U> decoder);

    /**
     * Creates the primitive.
     *
     * @return a future to be completed once the primitive has been created
     */
    CompletableFuture<Void> create();

    /**
     * Deletes the primitive.
     *
     * @return a future to be completed once the primitive has been deleted
     */
    CompletableFuture<Void> delete();

}
