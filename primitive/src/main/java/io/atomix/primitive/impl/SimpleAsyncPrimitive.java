package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.primitive.service.impl.StreamContext;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Simple asynchronous primitive.
 */
public abstract class SimpleAsyncPrimitive<P extends PrimitiveProxy> implements AsyncPrimitive {
  private final P proxy;
  private final AtomicLong index = new AtomicLong();

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

  protected <T, U> CompletableFuture<U> execute(
      OperationFunction<P, T, U> function,
      T request) {
    return function.execute(proxy, RequestContext.newBuilder()
        .setIndex(index.get())
        .build(), request)
        .thenApply(response -> {
          index.accumulateAndGet(response.getLeft().getIndex(), Math::max);
          return response.getRight();
        });
  }

  protected <T, U> CompletableFuture<Void> execute(
      OperationStreamFunction<P, T, U> function,
      T request,
      StreamHandler<U> handler) {
    return function.execute(proxy, RequestContext.newBuilder()
            .setIndex(index.get())
            .build(),
        request,
        new StreamHandler<Pair<StreamContext, U>>() {
          @Override
          public void next(Pair<StreamContext, U> response) {
            index.accumulateAndGet(response.getLeft().getIndex(), Math::max);
            handler.next(response.getRight());
          }

          @Override
          public void complete() {
            handler.complete();
          }

          @Override
          public void error(Throwable error) {
            handler.error(error);
          }
        });
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return proxy.delete();
  }
}
