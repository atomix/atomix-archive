package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.proxy.SimplePrimitiveProxy;

/**
 * Simple asynchronous primitive.
 */
public abstract class SimpleAsyncPrimitive<P extends SimplePrimitiveProxy> implements AsyncPrimitive {
  private final String name;
  private final PrimitiveType type;
  private final P proxy;

  public SimpleAsyncPrimitive(String name, PrimitiveType type, P proxy) {
    this.name = name;
    this.type = type;
    this.proxy = proxy;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public PrimitiveType type() {
    return type;
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
}
