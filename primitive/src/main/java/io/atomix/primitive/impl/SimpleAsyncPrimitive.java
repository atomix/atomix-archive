package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.concurrent.Futures;

/**
 * Simple asynchronous primitive.
 */
public abstract class SimpleAsyncPrimitive<P extends PrimitiveProxy> implements AsyncPrimitive {
  private final P proxy;

  public SimpleAsyncPrimitive(P proxy) {
    this.proxy = proxy;
  }

  @Override
  public String name() {
    return proxy.name();
  }

  /**
   * Returns the primitive proxy.
   *
   * @return the primitive proxy
   */
  protected P getProxy() {
    return proxy;
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }
}
