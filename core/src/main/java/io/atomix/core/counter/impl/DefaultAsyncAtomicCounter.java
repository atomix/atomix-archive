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
import java.util.function.Supplier;

import io.atomix.core.counter.AsyncAtomicCounter;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.CheckAndSetRequest;
import io.atomix.core.counter.CheckAndSetResponse;
import io.atomix.core.counter.CounterId;
import io.atomix.core.counter.CounterServiceGrpc;
import io.atomix.core.counter.CreateRequest;
import io.atomix.core.counter.CreateResponse;
import io.atomix.core.counter.DecrementRequest;
import io.atomix.core.counter.DecrementResponse;
import io.atomix.core.counter.DeleteRequest;
import io.atomix.core.counter.DeleteResponse;
import io.atomix.core.counter.GetRequest;
import io.atomix.core.counter.GetResponse;
import io.atomix.core.counter.IncrementRequest;
import io.atomix.core.counter.IncrementResponse;
import io.atomix.core.counter.SetRequest;
import io.atomix.core.counter.SetResponse;
import io.atomix.core.impl.PrimitiveIdDescriptor;
import io.atomix.core.impl.PrimitivePartition;
import io.atomix.core.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.DistributedLogProtocol;
import io.atomix.primitive.protocol.MultiPrimaryProtocol;
import io.atomix.primitive.protocol.MultiRaftProtocol;
import io.grpc.Channel;

/**
 * Atomix counter implementation.
 */
public class DefaultAsyncAtomicCounter
    extends AbstractAsyncPrimitive<CounterId, AsyncAtomicCounter>
    implements AsyncAtomicCounter {
  private final CounterServiceGrpc.CounterServiceStub counter;

  public DefaultAsyncAtomicCounter(
      CounterId id,
      Supplier<Channel> channelFactory,
      PrimitiveManagementService managementService) {
    super(id, COUNTER_ID_DESCRIPTOR, managementService, Partitioner.MURMUR3, Duration.ZERO);
    this.counter = CounterServiceGrpc.newStub(channelFactory.get());
  }

  @Override
  public CompletableFuture<Long> get() {
    PrimitivePartition partition = getPartition();
    return this.<GetResponse>execute(observer -> counter.get(GetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getValue(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> set(long value) {
    PrimitivePartition partition = getPartition();
    return this.<SetResponse>execute(observer -> counter.set(SetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .setValue(value)
        .build(), observer))
        .thenCompose(response -> partition.complete(null, response.getHeader()));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue) {
    PrimitivePartition partition = getPartition();
    return this.<CheckAndSetResponse>execute(observer -> counter.checkAndSet(CheckAndSetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .setExpect(expectedValue)
        .setUpdate(updateValue)
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getSucceeded(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Long> addAndGet(long delta) {
    PrimitivePartition partition = getPartition();
    return this.<IncrementResponse>execute(observer -> counter.increment(IncrementRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .setDelta(delta)
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getNextValue(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long delta) {
    PrimitivePartition partition = getPartition();
    return this.<IncrementResponse>execute(observer -> counter.increment(IncrementRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .setDelta(delta)
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getPreviousValue(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    PrimitivePartition partition = getPartition();
    return this.<IncrementResponse>execute(observer -> counter.increment(IncrementRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getNextValue(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    PrimitivePartition partition = getPartition();
    return this.<IncrementResponse>execute(observer -> counter.increment(IncrementRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getPreviousValue(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    PrimitivePartition partition = getPartition();
    return this.<DecrementResponse>execute(observer -> counter.decrement(DecrementRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getNextValue(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    PrimitivePartition partition = getPartition();
    return this.<DecrementResponse>execute(observer -> counter.decrement(DecrementRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .build(), observer))
        .thenCompose(response -> partition.complete(response.getPreviousValue(), response.getHeader()));
  }

  @Override
  public CompletableFuture<AsyncAtomicCounter> connect() {
    return this.<CreateResponse>execute(observer -> counter.create(CreateRequest.newBuilder()
        .setId(id())
        .build(), observer))
        .thenApply(response -> {
          getOrCreatePartition(response.getHeader().getPartitionId()).init(response.getHeader());
          return this;
        });
  }

  @Override
  protected CompletableFuture<Void> keepAlive() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    PrimitivePartition partition = getPartition();
    return this.<DeleteResponse>execute(observer -> counter.delete(DeleteRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getRequestHeader())
        .build(), observer))
        .thenApply(v -> null);
  }

  @Override
  public AtomicCounter sync(Duration operationTimeout) {
    return new BlockingAtomicCounter(this, operationTimeout.toMillis());
  }

  private static final PrimitiveIdDescriptor<CounterId> COUNTER_ID_DESCRIPTOR = new PrimitiveIdDescriptor<CounterId>() {
    @Override
    public String getName(CounterId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(CounterId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(CounterId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(CounterId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(CounterId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(CounterId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(CounterId id) {
      return id.getLog();
    }
  };
}