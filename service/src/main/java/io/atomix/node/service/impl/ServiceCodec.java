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
package io.atomix.node.service.impl;

import java.util.HashMap;
import java.util.Map;

import io.atomix.node.service.operation.OperationId;
import io.atomix.node.service.operation.StreamType;
import io.atomix.node.service.util.ByteArrayDecoder;
import io.atomix.node.service.util.ByteArrayEncoder;

/**
 * Service codec.
 */
public class ServiceCodec {
    private final Map<OperationId, OperationCodec> operations = new HashMap<>();
    private final Map<StreamType, StreamCodec> streams = new HashMap<>();

    public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId) {
        return register(operationId, new OperationCodec<>(null, null));
    }

    public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, ByteArrayDecoder<T> decoder) {
        return register(operationId, new OperationCodec<>(decoder, null));
    }

    public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, ByteArrayEncoder<U> encoder) {
        return register(operationId, new OperationCodec<>(null, encoder));
    }

    public <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, ByteArrayDecoder<T> decoder, ByteArrayEncoder<U> encoder) {
        return register(operationId, new OperationCodec<>(decoder, encoder));
    }

    private <T, U> OperationCodec<T, U> register(OperationId<T, U> operationId, OperationCodec<T, U> operation) {
        operations.put(operationId, operation);
        return operation;
    }

    public <T> StreamCodec<T> register(StreamType<T> streamType, ByteArrayEncoder<T> encoder) {
        return register(streamType, new StreamCodec<>(encoder));
    }

    private <T> StreamCodec<T> register(StreamType<T> streamType, StreamCodec<T> codec) {
        streams.put(streamType, codec);
        return codec;
    }

    @SuppressWarnings("unchecked")
    public <T, U> OperationCodec<T, U> getOperation(OperationId<T, U> operationId) {
        return operations.get(operationId);
    }

    @SuppressWarnings("unchecked")
    public <T> StreamCodec<T> getStream(StreamType<T> streamType) {
        return streams.get(streamType);
    }

    @SuppressWarnings("unchecked")
    public <T> T decode(OperationId<T, ?> operationId, byte[] bytes) {
        return (T) operations.get(operationId).decode(bytes);
    }

    @SuppressWarnings("unchecked")
    public <U> byte[] encode(OperationId<?, U> operationId, U value) {
        return operations.get(operationId).encode(value);
    }

    @SuppressWarnings("unchecked")
    public <T> byte[] encode(StreamType<T> streamType, T value) {
        return streams.get(streamType).encode(value);
    }
}
