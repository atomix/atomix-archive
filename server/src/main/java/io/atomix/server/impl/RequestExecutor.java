package io.atomix.server.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Strings;
import com.google.protobuf.Message;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.service.proxy.ServiceProxy;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;

/**
 * Primitive factory.
 */
public class RequestExecutor<P extends ServiceProxy> {
  private final PrimitiveFactory<P> primitiveFactory;

  public RequestExecutor(PrimitiveFactory<P> primitiveFactory) {
    this.primitiveFactory = primitiveFactory;
  }

  /**
   * Executes the given function on the primitive.
   *
   * @param id               the primitive ID
   * @param responseObserver the response observer
   * @param function         the function to execute
   */
  public <R extends Message> void execute(
      PrimitiveId id,
      Supplier<R> defaultResponseSupplier,
      StreamObserver<R> responseObserver,
      Function<P, CompletableFuture<R>> function) {
    if (isValidId(id, defaultResponseSupplier, responseObserver)) {
      function.apply(primitiveFactory.getPrimitive(id)).whenComplete((result, funcError) -> {
        if (funcError == null) {
          responseObserver.onNext(result);
          responseObserver.onCompleted();
        } else {
          responseObserver.onError(funcError);
        }
      });
    }
  }

  /**
   * Executes the given function on the primitive.
   *
   * @param id               the primitive ID
   * @param responseObserver the response observer
   * @param function         the function to execute
   */
  public <T, R extends Message> void execute(
      PrimitiveId id,
      Supplier<R> defaultResponseSupplier,
      StreamObserver<R> responseObserver,
      BiFunction<P, StreamHandler<T>, CompletableFuture<?>> function,
      Function<T, R> converter) {
    if (isValidId(id, defaultResponseSupplier, responseObserver)) {
      StreamHandler<T> handler = new StreamHandler<T>() {
        @Override
        public void next(T response) {
          responseObserver.onNext(converter.apply(response));
        }

        @Override
        public void complete() {
          responseObserver.onCompleted();
        }

        @Override
        public void error(Throwable error) {
          responseObserver.onError(error);
        }
      };

      function.apply(primitiveFactory.getPrimitive(id), handler).whenComplete((result, funcError) -> {
        if (funcError != null) {
          responseObserver.onError(funcError);
        }
      });
    }
  }

  /**
   * Validates the given ID.
   *
   * @param id                      the primitive ID
   * @param defaultResponseSupplier the default response supplier
   * @param responseObserver        the response observer
   * @return indicates whether the ID is valid
   */
  private <R extends Message> boolean isValidId(PrimitiveId id, Supplier<R> defaultResponseSupplier, StreamObserver<R> responseObserver) {
    if (Strings.isNullOrEmpty(id.getName())) {
      fail(Status.INVALID_ARGUMENT, "Primitive name not specified", defaultResponseSupplier, responseObserver);
      return false;
    }
    return true;
  }

  /**
   * Sends a failure response to the given observer.
   *
   * @param status                  the response status
   * @param message                 the failure message
   * @param defaultResponseSupplier the default response supplier
   * @param responseObserver        the response observer on which to send the error
   */
  private <R extends Message> void fail(Status status, String message, Supplier<R> defaultResponseSupplier, StreamObserver<R> responseObserver) {
    R response = defaultResponseSupplier.get();
    Metadata.Key<R> key = ProtoUtils.keyForProto(response);
    Metadata metadata = new Metadata();
    metadata.put(key, response);
    responseObserver.onError(status.withDescription(message)
        .asRuntimeException(metadata));
  }
}
