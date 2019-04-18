package io.atomix.core.iterator.impl;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import io.atomix.core.iterator.AsyncIterator;

/**
 * Default asynchronous iterator.
 *
 * @param <T> the value type
 */
public class DefaultAsyncIterator<T> implements AsyncIterator<T> {
  private final BiConsumer<Long, DefaultAsyncIterator<T>> completeFunction;
  private final CompletableFuture<Void> init;
  private final AtomicInteger received = new AtomicInteger();
  private final AtomicInteger checked = new AtomicInteger();
  private volatile long index;
  private volatile int size;
  private final Queue<CompletableFuture<T>> queue = new LinkedList<>();
  private final Queue<CompletableFuture<T>> next = new LinkedList<>();

  public DefaultAsyncIterator(
      Supplier<CompletableFuture<Context>> contextFactory,
      BiConsumer<Long, DefaultAsyncIterator<T>> initFunction,
      BiConsumer<Long, DefaultAsyncIterator<T>> completeFunction) {
    this.completeFunction = completeFunction;
    this.init = contextFactory.get()
        .thenAccept(context -> {
          index = context.index();
          size = context.size();
          if (context.size() > 0) {
            initFunction.accept(index, this);
          }
        });
  }

  /**
   * Adds a value to the iterator.
   *
   * @param value the value to add
   */
  public void add(T value) {
    CompletableFuture<T> future;
    synchronized (this) {
      future = next.poll();
      if (future == null) {
        future = new CompletableFuture<>();
        queue.add(future);
      }
    }
    future.complete(value);
    if (received.incrementAndGet() == size) {
      completeFunction.accept(index, this);
    }
  }

  @Override
  public CompletableFuture<Boolean> hasNext() {
    return init.thenApply(v -> checked.incrementAndGet() <= size);
  }

  @Override
  public CompletableFuture<T> next() {
    return init.thenCompose(v -> {
      CompletableFuture<T> future;
      synchronized (this) {
        future = queue.poll();
        if (future == null) {
          future = new CompletableFuture<>();
          next.add(future);
        }
      }
      return future;
    });
  }

  /**
   * Iterator context.
   */
  public static class Context {
    private final long index;
    private final int size;

    public Context(long index, int size) {
      this.index = index;
      this.size = size;
    }

    /**
     * Returns the iterator index.
     *
     * @return the iterator index
     */
    public long index() {
      return index;
    }

    /**
     * Returns the iterator size.
     *
     * @return the iterator size
     */
    public int size() {
      return size;
    }
  }
}