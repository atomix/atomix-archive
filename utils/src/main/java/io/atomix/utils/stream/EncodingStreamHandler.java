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
package io.atomix.utils.stream;

import java.util.function.Function;

import io.atomix.utils.stream.StreamHandler;

/**
 * Transcoding stream handler.
 */
public class EncodingStreamHandler<T, U> implements StreamHandler<T> {
    private final StreamHandler<U> handler;
    private final Function<T, U> transcoder;

    public EncodingStreamHandler(StreamHandler<U> handler, Function<T, U> transcoder) {
        this.handler = handler;
        this.transcoder = transcoder;
    }

    @Override
    public void next(T value) {
        handler.next(transcoder.apply(value));
    }

    @Override
    public void complete() {
        handler.complete();
    }

    @Override
    public void error(Throwable error) {
        handler.error(error);
    }
}
