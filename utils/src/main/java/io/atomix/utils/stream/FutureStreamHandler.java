package io.atomix.utils.stream;

import java.util.concurrent.CompletableFuture;

/**
 * Stream handler that completes a future.
 */
public class FutureStreamHandler<T> implements StreamHandler<T> {
  private final CompletableFuture<T> future;

  public FutureStreamHandler(CompletableFuture<T> future) {
    this.future = future;
  }

  @Override
  public void next(T value) {
    future.complete(value);
  }

  @Override
  public void complete() {
    future.complete(null);
  }

  @Override
  public void error(Throwable error) {
    future.completeExceptionally(error);
  }
}
