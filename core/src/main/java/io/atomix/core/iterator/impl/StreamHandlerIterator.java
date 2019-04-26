package io.atomix.core.iterator.impl;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.atomix.core.iterator.AsyncIterator;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;

/**
 * Stream handler iterator.
 */
public class StreamHandlerIterator<T> implements StreamHandler<T>, AsyncIterator<T> {
  private final Queue<CompletableFuture<T>> queue = new ConcurrentLinkedQueue<>();
  private volatile CompletableFuture<T> nextFuture;

  @Override
  public synchronized CompletableFuture<Boolean> hasNext() {
    CompletableFuture<T> future = queue.peek();
    if (future == null) {
      if (nextFuture == null) {
        nextFuture = new CompletableFuture<>();
        queue.add(nextFuture);
      }
      future = nextFuture;
    }
    return future.thenApply(value -> value != null);
  }

  @Override
  public synchronized CompletableFuture<T> next() {
    CompletableFuture<T> future = queue.poll();
    if (future != null) {
      return future;
    } else if (nextFuture != null) {
      return nextFuture;
    } else {
      nextFuture = new CompletableFuture<>();
      return nextFuture;
    }
  }

  @Override
  public synchronized void next(T value) {
    if (nextFuture != null) {
      nextFuture.complete(value);
      nextFuture = null;
    } else {
      queue.add(CompletableFuture.completedFuture(value));
    }
  }

  @Override
  public synchronized void complete() {
    if (nextFuture != null) {
      nextFuture.complete(null);
    } else {
      queue.add(CompletableFuture.completedFuture(null));
    }
  }

  @Override
  public synchronized void error(Throwable error) {
    if (nextFuture != null) {
      nextFuture.completeExceptionally(error);
    } else {
      queue.add(Futures.exceptionalFuture(error));
    }
  }
}
