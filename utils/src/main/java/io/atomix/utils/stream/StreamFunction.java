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

/**
 * Stream function.
 */
public interface StreamFunction<T, U> {

    /**
     * Called to handle the next value.
     *
     * @param value the value to handle
     */
    void next(T value);

    /**
     * Called when the stream is complete.
     *
     * @return the result
     */
    U complete();

    /**
     * Called when a stream error occurs.
     *
     * @param error the stream error
     * @return the result
     */
    U error(Throwable error);

}
