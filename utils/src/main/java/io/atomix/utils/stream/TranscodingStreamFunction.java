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

import io.atomix.utils.stream.StreamFunction;

/**
 * Transcoding stream handler.
 */
public class TranscodingStreamFunction<T1, U1, T2, U2> implements StreamFunction<T2, U2> {
    private final StreamFunction<T1, U1> function;
    private final Function<T2, T1> encoder;
    private final Function<U1, U2> decoder;

    public TranscodingStreamFunction(StreamFunction<T1, U1> function, Function<T2, T1> encoder, Function<U1, U2> decoder) {
        this.function = function;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public void next(T2 value) {
        function.next(encoder.apply(value));
    }

    @Override
    public U2 complete() {
        return decoder.apply(function.complete());
    }

    @Override
    public U2 error(Throwable error) {
        return decoder.apply(function.error(error));
    }
}
