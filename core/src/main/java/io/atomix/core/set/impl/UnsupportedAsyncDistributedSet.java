package io.atomix.core.set.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.atomix.core.collection.impl.UnsupportedAsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.utils.concurrent.Futures;

/**
 * Distributed set implementation that returns {@link UnsupportedOperationException} for all asynchronous operations.
 */
public abstract class UnsupportedAsyncDistributedSet<E>
    extends UnsupportedAsyncDistributedCollection<E>
    implements AsyncDistributedSet<E> {

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
