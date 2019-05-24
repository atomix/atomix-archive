package io.atomix.client.set.impl;

import java.time.Duration;

import io.atomix.client.collection.impl.UnsupportedAsyncDistributedCollection;
import io.atomix.client.set.AsyncDistributedSet;
import io.atomix.client.set.DistributedSet;

/**
 * Distributed set implementation that returns {@link UnsupportedOperationException} for all asynchronous operations.
 */
public abstract class UnsupportedAsyncDistributedSet<E>
    extends UnsupportedAsyncDistributedCollection<E>
    implements AsyncDistributedSet<E> {

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
