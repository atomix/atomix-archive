/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.core.value.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueBuilder;
import io.atomix.core.value.AtomicValueConfig;
import io.atomix.primitive.ManagedAsyncPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.utils.serializer.Serializer;

/**
 * Default implementation of AtomicValueBuilder.
 *
 * @param <V> value type
 */
public class DefaultAtomicValueBuilder<V> extends AtomicValueBuilder<V> {
  public DefaultAtomicValueBuilder(String name, AtomicValueConfig config, PrimitiveManagementService managementService) {
    super(name, config, managementService);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<AtomicValue<V>> buildAsync() {
    return managementService.getPrimitiveRegistry().createPrimitive(name, type)
        .thenApply(v -> newSingletonProxy(ValueService.TYPE, ValueProxy::new))
        .thenApply(proxy -> new RawAsyncAtomicValue(proxy, config.getSessionTimeout(), managementService))
        .thenCompose(ManagedAsyncPrimitive::connect)
        .thenApply(rawValue -> {
          Serializer serializer = serializer();
          return new TranscodingAsyncAtomicValue<V, byte[]>(
              rawValue,
              key -> serializer.encode(key),
              bytes -> serializer.decode(bytes));
        })
        .thenApply(AsyncAtomicValue::sync);
  }
}
