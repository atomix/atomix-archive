package io.atomix.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.atomix.api.headers.Name;
import io.atomix.api.headers.RequestHeader;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.client.AsyncPrimitive;
import io.atomix.client.ManagedAsyncPrimitive;
import io.atomix.client.utils.concurrent.ThreadContext;
import io.grpc.stub.StreamObserver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple asynchronous primitive.
 */
public abstract class AbstractAsyncPrimitive<S, P extends AsyncPrimitive> implements ManagedAsyncPrimitive<P> {
  private final Name name;
  private final S service;
  private final ThreadContext context;
  private final AtomicLong index = new AtomicLong();

  public AbstractAsyncPrimitive(Name name, S service, ThreadContext context) {
    this.name = checkNotNull(name);
    this.service = checkNotNull(service);
    this.context = context;
  }

  @Override
  public String name() {
    return name.getName();
  }

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  protected Name getName() {
    return name;
  }

  /**
   * Returns the primitive thread context.
   *
   * @return the primitive thread context
   */
  protected ThreadContext context() {
    return context;
  }

  /**
   * Returns the primitive service.
   *
   * @return the primitive service
   */
  protected S getService() {
    return service;
  }

  private RequestHeader getRequestHeader() {
    return RequestHeader.newBuilder()
        .setName(name)
        .setIndex(index.get())
        .build();
  }

  protected <T> CompletableFuture<T> execute(
      BiConsumer<RequestHeader, StreamObserver<T>> callback,
      Function<T, ResponseHeader> headerFunction) {
    CompletableFuture<T> future = new CompletableFuture<>();
    callback.accept(getRequestHeader(), new StreamObserver<T>() {
      @Override
      public void onNext(T response) {
        index.accumulateAndGet(headerFunction.apply(response).getIndex(), Math::max);
        future.complete(response);
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
      }
    });
    return future;
  }

  protected <T> CompletableFuture<Void> execute(
      BiConsumer<RequestHeader, StreamObserver<T>> callback,
      Function<T, ResponseHeader> headerFunction,
      StreamObserver<T> observer) {
    callback.accept(getRequestHeader(), new StreamObserver<T>() {
      @Override
      public void onNext(T response) {
        index.accumulateAndGet(headerFunction.apply(response).getIndex(), Math::max);
        observer.onNext(response);
      }

      @Override
      public void onError(Throwable t) {
        observer.onError(t);
      }

      @Override
      public void onCompleted() {
        observer.onCompleted();
      }
    });
    return CompletableFuture.completedFuture(null);
  }
}