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

package io.atomix.node.service;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.node.service.operation.OperationId;
import io.atomix.node.service.operation.StreamType;
import io.atomix.node.service.util.ByteArrayDecoder;
import io.atomix.node.service.util.ByteArrayEncoder;
import io.atomix.utils.stream.StreamHandler;

/**
 * Facilitates registration and execution of state machine commands and provides deterministic scheduling.
 *
 * @see PrimitiveService
 */
public interface ServiceOperationRegistry {

    /**
     * Registers a operation callback.
     *
     * @param operationId the operation identifier
     * @param callback    the operation callback
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    void register(OperationId<Void, Void> operationId, Runnable callback);

    /**
     * Registers a no argument operation callback.
     *
     * @param operationId the operation identifier
     * @param callback    the operation callback
     * @param encoder     the response encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    <R> void register(OperationId<Void, R> operationId, Supplier<R> callback, ByteArrayEncoder<R> encoder);

    /**
     * Registers a operation callback.
     *
     * @param operationId the operation identifier
     * @param callback    the operation callback
     * @param decoder     the operation decoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    <T> void register(OperationId<T, Void> operationId, Consumer<T> callback, ByteArrayDecoder<T> decoder);

    /**
     * Registers an operation callback.
     *
     * @param operationId the operation identifier
     * @param callback    the operation callback
     * @param decoder     the operation decoder
     * @param encoder     the response encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    <T, R> void register(OperationId<T, R> operationId, Function<T, R> callback, ByteArrayDecoder<T> decoder, ByteArrayEncoder<R> encoder);

    /**
     * Registers an asynchronous operation callback.
     *
     * @param operationId the operation identifier
     * @param callback    the operation callback
     * @param decoder     the operation decoder
     * @param encoder     the response encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    <T, R> void register(OperationId<T, R> operationId, StreamType<R> streamType, Function<T, CompletableFuture<R>> callback, ByteArrayDecoder<T> decoder, ByteArrayEncoder<R> encoder);

    /**
     * Registers an operation callback.
     *
     * @param operationId the operation identifier
     * @param streamType  the stream type
     * @param callback    the operation callback
     * @param encoder     the response encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    <R> void register(OperationId<Void, R> operationId, StreamType<R> streamType, Consumer<StreamHandler<R>> callback, ByteArrayEncoder<R> encoder);

    /**
     * Registers an operation callback.
     *
     * @param operationId the operation identifier
     * @param streamType  the stream type
     * @param callback    the operation callback
     * @param decoder     the operation decoder
     * @param encoder     the response encoder
     * @throws NullPointerException if the {@code operationId} or {@code callback} is null
     */
    <T, R> void register(OperationId<T, R> operationId, StreamType<R> streamType, BiConsumer<T, StreamHandler<R>> callback, ByteArrayDecoder<T> decoder, ByteArrayEncoder<R> encoder);

}