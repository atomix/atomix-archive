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

import java.util.concurrent.CompletableFuture;

/**
 * Stream handler that completes a future.
 */
public class FutureStreamHandler<T> implements StreamHandler<T> {
    private final CompletableFuture<T> future;

    public FutureStreamHandler(CompletableFuture<T> future) {
        this.future = future;
    }

    @Override
    public void next(T value) {
        future.complete(value);
    }

    @Override
    public void complete() {
        future.complete(null);
    }

    @Override
    public void error(Throwable error) {
        future.completeExceptionally(error);
    }
}
