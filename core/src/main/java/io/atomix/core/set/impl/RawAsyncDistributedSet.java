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
package io.atomix.core.set.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.StreamHandlerIterator;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.impl.SessionEnabledAsyncPrimitive;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.EncodingStreamHandler;
import io.atomix.utils.stream.StreamHandler;

/**
 * Raw async distributed set.
 */
public class RawAsyncDistributedSet extends SessionEnabledAsyncPrimitive<SetProxy, AsyncDistributedSet<String>> implements AsyncDistributedSet<String> {
  private final Map<CollectionEventListener<String>, Executor> eventListeners = Maps.newConcurrentMap();
  private volatile long streamId;

  public RawAsyncDistributedSet(SetProxy proxy, Duration timeout, PrimitiveManagementService managementService) {
    super(proxy, timeout, managementService);
  }

  @Override
  public CompletableFuture<Boolean> add(String element) {
    return execute(SetProxy::add, AddRequest.newBuilder()
        .addValues(element)
        .build())
        .thenApply(response -> response.getAdded());
  }

  @Override
  public CompletableFuture<Boolean> remove(String element) {
    return execute(SetProxy::remove, RemoveRequest.newBuilder()
        .addValues(element)
        .build())
        .thenApply(response -> response.getRemoved());
  }

  @Override
  public CompletableFuture<Integer> size() {
    return execute(SetProxy::size, SizeRequest.newBuilder().build())
        .thenApply(response -> response.getSize());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return execute(SetProxy::clear, ClearRequest.newBuilder().build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> contains(String element) {
    return execute(SetProxy::contains, ContainsRequest.newBuilder()
        .addValues(element)
        .build())
        .thenApply(response -> response.getContains());
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
    return execute(SetProxy::add, AddRequest.newBuilder()
        .addAllValues((Iterable<String>) c)
        .build())
        .thenApply(response -> response.getAdded());
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
    return execute(SetProxy::contains, ContainsRequest.newBuilder()
        .addAllValues((Iterable<String>) c)
        .build())
        .thenApply(response -> response.getContains());
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
    return execute(SetProxy::remove, RemoveRequest.newBuilder()
        .addAllValues((Iterable<String>) c)
        .build())
        .thenApply(response -> response.getRemoved());
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
    if (eventListeners.putIfAbsent(listener, executor) == null) {
      return execute(SetProxy::listen, ListenRequest.newBuilder().build(), new StreamHandler<ListenResponse>() {
        @Override
        public void next(ListenResponse response) {
          eventListeners.forEach((l, e) -> e.execute(() -> l.event(new CollectionEvent<>(
              CollectionEvent.Type.valueOf(response.getType().name()), response.getValue()))));
        }

        @Override
        public void complete() {

        }

        @Override
        public void error(Throwable error) {

        }
      }).thenApply(streamId -> {
        this.streamId = streamId;
        return null;
      });
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return execute(SetProxy::unlisten, UnlistenRequest.newBuilder().setStreamId(streamId).build())
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public AsyncIterator<String> iterator() {
    StreamHandlerIterator<String> iterator = new StreamHandlerIterator<>();
    execute(
        SetProxy::iterate,
        IterateRequest.newBuilder().build(),
        new EncodingStreamHandler<>(iterator, response -> response.getValue()));
    return iterator;
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<String>> transactionLog) {
    return execute(SetProxy::prepare, PrepareRequest.newBuilder()
        .setTransactionId(transactionLog.transactionId().id())
        .setTransaction(DistributedSetTransaction.newBuilder()
            .setVersion(transactionLog.version())
            .addAllUpdates(transactionLog.records().stream()
                .map(update -> DistributedSetUpdate.newBuilder()
                    .setType(DistributedSetUpdate.Type.valueOf(update.type().name()))
                    .setValue(update.element())
                    .build())
                .collect(Collectors.toList()))
            .build())
        .build())
        .thenApply(response -> response.getStatus() == PrepareResponse.Status.OK);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return execute(SetProxy::commit, CommitRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return execute(SetProxy::rollback, RollbackRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public DistributedSet<String> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}