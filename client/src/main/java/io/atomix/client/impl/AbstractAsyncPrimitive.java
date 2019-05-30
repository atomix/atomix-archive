package io.atomix.client.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import io.atomix.api.headers.RequestHeader;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.AsyncPrimitive;
import io.atomix.client.ManagedAsyncPrimitive;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.utils.concurrent.ThreadContext;
import io.grpc.stub.StreamObserver;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Simple asynchronous primitive.
 */
public abstract class AbstractAsyncPrimitive<S, P extends AsyncPrimitive> implements ManagedAsyncPrimitive<P> {
  private final PrimitiveId id;
  private final S service;
  private final ThreadContext context;
  private final AtomicLong index = new AtomicLong();

  public AbstractAsyncPrimitive(PrimitiveId id, S service, PrimitiveManagementService managementService) {
    this.id = checkNotNull(id);
    this.service = checkNotNull(service);
    this.context = managementService.getThreadFactory().createContext();
  }

  @Override
  public String name() {
    return id.getName();
  }

  /**
   * Returns the primitive ID.
   *
   * @return the primitive ID
   */
  protected PrimitiveId getPrimitiveId() {
    return id;
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
        .setIndex(index.get())
        .build();
  }

  protected <T> CompletableFuture<T> execute(
      OperationFunction<S, T> function,
      Function<T, ResponseHeader> headerFunction) {
    CompletableFuture<T> future = new CompletableFuture<>();
    function.apply(getService(), getRequestHeader(), new StreamObserver<T>() {
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
      OperationFunction<S, T> function,
      Function<T, ResponseHeader> headerFunction,
      StreamObserver<T> observer) {
    function.apply(getService(), getRequestHeader(), new StreamObserver<T>() {
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