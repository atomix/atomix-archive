package io.atomix.core.collection.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.utils.concurrent.Futures;

/**
 * Distributed collection that returns {@link UnsupportedOperationException} for all asynchronous operations.
 */
public abstract class UnsupportedAsyncDistributedCollection<E> implements AsyncDistributedCollection<E> {
  @Override
  public CompletableFuture<Boolean> add(E element) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> remove(E element) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Integer> size() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> contains(E element) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> addAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends E> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> addListener(CollectionEventListener<E> listener, Executor executor) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> removeListener(CollectionEventListener<E> listener) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> close() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public DistributedCollection<E> sync(Duration operationTimeout) {
    return new BlockingDistributedCollection<>(this, operationTimeout.toMillis());
  }
}
