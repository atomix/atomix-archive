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
package io.atomix.client.counter.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.atomix.api.counter.CheckAndSetRequest;
import io.atomix.api.counter.CheckAndSetResponse;
import io.atomix.api.counter.CloseRequest;
import io.atomix.api.counter.CloseResponse;
import io.atomix.api.counter.CounterServiceGrpc;
import io.atomix.api.counter.CreateRequest;
import io.atomix.api.counter.CreateResponse;
import io.atomix.api.counter.DecrementRequest;
import io.atomix.api.counter.DecrementResponse;
import io.atomix.api.counter.GetRequest;
import io.atomix.api.counter.GetResponse;
import io.atomix.api.counter.IncrementRequest;
import io.atomix.api.counter.IncrementResponse;
import io.atomix.api.counter.SetRequest;
import io.atomix.api.counter.SetResponse;
import io.atomix.api.headers.Name;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.client.counter.AsyncAtomicCounter;
import io.atomix.client.counter.AtomicCounter;
import io.atomix.client.impl.AbstractAsyncPrimitive;
import io.atomix.client.partition.Partition;
import io.atomix.client.utils.concurrent.ThreadContext;

/**
 * Atomix counter implementation.
 */
public class DefaultAsyncAtomicCounter
    extends AbstractAsyncPrimitive<CounterServiceGrpc.CounterServiceStub, AsyncAtomicCounter>
    implements AsyncAtomicCounter {
  public DefaultAsyncAtomicCounter(Name name, Partition partition, ThreadContext context) {
    super(name, CounterServiceGrpc.newStub(partition.getChannelFactory().getChannel()), context);
  }

  @Override
  public CompletableFuture<Long> get() {
    return execute((header, observer) -> getService().get(GetRequest.newBuilder()
        .setHeader(header)
        .build(), observer), GetResponse::getHeader)
        .thenApply(response -> response.getValue());
  }

  @Override
  public CompletableFuture<Void> set(long value) {
    return execute((header, observer) -> getService().set(SetRequest.newBuilder()
        .setHeader(header)
        .setValue(value)
        .build(), observer), SetResponse::getHeader)
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue) {
    return execute((header, observer) -> getService().checkAndSet(CheckAndSetRequest.newBuilder()
        .setHeader(header)
        .setExpect(expectedValue)
        .setUpdate(updateValue)
        .build(), observer), CheckAndSetResponse::getHeader)
        .thenApply(response -> response.getSucceeded());
  }

  @Override
  public CompletableFuture<Long> addAndGet(long delta) {
    return execute((header, observer) -> getService().increment(IncrementRequest.newBuilder()
        .setHeader(header)
        .setDelta(delta)
        .build(), observer), IncrementResponse::getHeader)
        .thenApply(response -> response.getNextValue());
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long delta) {
    return execute((header, observer) -> getService().increment(IncrementRequest.newBuilder()
        .setHeader(header)
        .setDelta(delta)
        .build(), observer), IncrementResponse::getHeader)
        .thenApply(response -> response.getPreviousValue());
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return execute((header, observer) -> getService().increment(IncrementRequest.newBuilder()
        .setHeader(header)
        .setDelta(1)
        .build(), observer), IncrementResponse::getHeader)
        .thenApply(response -> response.getNextValue());
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return execute((header, observer) -> getService().increment(IncrementRequest.newBuilder()
        .setHeader(header)
        .setDelta(1)
        .build(), observer), IncrementResponse::getHeader)
        .thenApply(response -> response.getPreviousValue());
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    return execute((header, observer) -> getService().decrement(DecrementRequest.newBuilder()
        .setHeader(header)
        .setDelta(1)
        .build(), observer), DecrementResponse::getHeader)
        .thenApply(response -> response.getNextValue());
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    return execute((header, observer) -> getService().decrement(DecrementRequest.newBuilder()
        .setHeader(header)
        .setDelta(1)
        .build(), observer), DecrementResponse::getHeader)
        .thenApply(response -> response.getPreviousValue());
  }

  @Override
  public CompletableFuture<AsyncAtomicCounter> connect() {
    return execute((header, observer) -> getService().create(CreateRequest.newBuilder()
        .build(), observer), CreateResponse::getHeader)
        .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return this.<CloseResponse>execute((header, observer) -> getService().close(CloseRequest.newBuilder()
        .build(), observer), response -> ResponseHeader.getDefaultInstance())
        .thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return this.<CloseResponse>execute((header, observer) -> getService().close(CloseRequest.newBuilder()
        .setDelete(true)
        .build(), observer), response -> ResponseHeader.getDefaultInstance())
        .thenApply(v -> null);
  }

  @Override
  public AtomicCounter sync(Duration operationTimeout) {
    return new BlockingAtomicCounter(this, operationTimeout.toMillis());
  }
}