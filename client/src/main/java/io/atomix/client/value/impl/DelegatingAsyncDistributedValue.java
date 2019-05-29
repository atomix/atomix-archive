/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.client.value.impl;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Maps;
import io.atomix.client.impl.DelegatingAsyncPrimitive;
import io.atomix.client.value.AsyncAtomicValue;
import io.atomix.client.value.AsyncDistributedValue;
import io.atomix.client.value.AtomicValueEventListener;
import io.atomix.client.value.DistributedValue;
import io.atomix.client.value.ValueEvent;
import io.atomix.client.value.ValueEventListener;
import io.atomix.client.Versioned;

/**
 * Delegating distributed value.
 */
public class DelegatingAsyncDistributedValue<V> extends DelegatingAsyncPrimitive<AsyncAtomicValue<V>> implements AsyncDistributedValue<V> {
  private final Map<ValueEventListener<V>, AtomicValueEventListener<V>> listenerMap = Maps.newConcurrentMap();

  public DelegatingAsyncDistributedValue(AsyncAtomicValue<V> primitive) {
    super(primitive);
  }

  @Override
  public CompletableFuture<V> get() {
    return delegate().get().thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<V> getAndSet(V value) {
    return delegate().getAndSet(value).thenApply(Versioned::valueOrNull);
  }

  @Override
  public CompletableFuture<Void> set(V value) {
    return delegate().set(value).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> addListener(ValueEventListener<V> listener) {
    AtomicValueEventListener<V> eventListener = event -> listener.event(new ValueEvent<>(
        ValueEvent.Type.valueOf(event.type().name()),
        Versioned.valueOrNull(event.newValue()),
        Versioned.valueOrNull(event.oldValue())));
    if (listenerMap.putIfAbsent(listener, eventListener) == null) {
      return delegate().addListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(ValueEventListener<V> listener) {
    AtomicValueEventListener<V> eventListener = listenerMap.remove(listener);
    if (eventListener != null) {
      return delegate().removeListener(eventListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public DistributedValue<V> sync(Duration operationTimeout) {
    return new BlockingDistributedValue<>(this, operationTimeout.toMillis());
  }
}
