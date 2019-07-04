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
package io.atomix.utils.component;

import java.util.concurrent.CompletableFuture;

/**
 * Managed component.
 */
public interface Managed {

    /**
     * Starts the managed component.
     *
     * @return a future to be completed once the component has been started
     */
    default CompletableFuture<Void> start() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Stops the managed component.
     *
     * @return a future to be completed once the component has been stopped
     */
    default CompletableFuture<Void> stop() {
        return CompletableFuture.completedFuture(null);
    }

}
