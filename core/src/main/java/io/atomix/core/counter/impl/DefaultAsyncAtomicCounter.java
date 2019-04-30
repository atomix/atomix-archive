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
package io.atomix.core.counter.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.atomix.core.counter.AsyncAtomicCounter;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.impl.SimpleAsyncPrimitive;

/**
 * Atomix counter implementation.
 */
public class DefaultAsyncAtomicCounter extends SimpleAsyncPrimitive<CounterProxy> implements AsyncAtomicCounter {
  public DefaultAsyncAtomicCounter(CounterProxy proxy) {
    super(proxy);
  }

  @Override
  public CompletableFuture<Long> get() {
    return execute(CounterProxy::get, GetRequest.newBuilder().build())
        .thenApply(response -> response.getValue());
  }

  @Override
  public CompletableFuture<Void> set(long value) {
    return execute(CounterProxy::set, SetRequest.newBuilder().setValue(value).build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue) {
    return execute(CounterProxy::checkAndSet, CheckAndSetRequest.newBuilder()
        .setExpect(expectedValue)
        .setUpdate(updateValue)
        .build())
        .thenApply(response -> response.getSucceeded());
  }

  @Override
  public CompletableFuture<Long> addAndGet(long delta) {
    return execute(CounterProxy::increment, IncrementRequest.newBuilder()
        .setDelta(delta)
        .build())
        .thenApply(response -> response.getNextValue());
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long delta) {
    return execute(CounterProxy::increment, IncrementRequest.newBuilder()
        .setDelta(delta)
        .build())
        .thenApply(response -> response.getPreviousValue());
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return execute(CounterProxy::increment, IncrementRequest.newBuilder().build())
        .thenApply(response -> response.getNextValue());
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return execute(CounterProxy::increment, IncrementRequest.newBuilder().build())
        .thenApply(response -> response.getPreviousValue());
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    return execute(CounterProxy::decrement, DecrementRequest.newBuilder().build())
        .thenApply(response -> response.getNextValue());
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    return execute(CounterProxy::decrement, DecrementRequest.newBuilder().build())
        .thenApply(response -> response.getPreviousValue());
  }

  @Override
  public AtomicCounter sync(Duration operationTimeout) {
    return new BlockingAtomicCounter(this, operationTimeout.toMillis());
  }
}