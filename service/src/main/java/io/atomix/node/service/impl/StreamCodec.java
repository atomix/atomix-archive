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

import io.atomix.node.service.util.ByteArrayEncoder;

/**
 * Stream codec.
 *
 * @param <T> the stream type
 */
public class StreamCodec<T> {
    private final ByteArrayEncoder<T> encoder;

    StreamCodec(ByteArrayEncoder<T> encoder) {
        this.encoder = encoder != null ? encoder : v -> null;
    }

    /**
     * Encodes the given value for the stream.
     *
     * @param value the value to encode
     * @return the encoded value
     */
    public byte[] encode(T value) {
        return ByteArrayEncoder.encode(value, encoder);
    }
}