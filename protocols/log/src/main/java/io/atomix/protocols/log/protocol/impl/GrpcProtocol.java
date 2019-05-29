package io.atomix.protocols.log.protocol.impl;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import io.atomix.protocols.log.protocol.DistributedLogServiceGrpc;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.LogServerProtocol;
import io.atomix.server.management.ServiceFactory;
import io.atomix.server.management.ServiceRegistry;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.stub.StreamObserver;

/**
 * gRPC based server protocol.
 */
public class GrpcProtocol extends DistributedLogServiceGrpc.DistributedLogServiceImplBase implements LogClientProtocol, LogServerProtocol {
  private static final ConnectException CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private final ServiceFactory<DistributedLogServiceGrpc.DistributedLogServiceStub> factory;

  public GrpcProtocol(ServiceFactory<DistributedLogServiceGrpc.DistributedLogServiceStub> factory, ServiceRegistry registry) {
    this.factory = factory;
    registry.register(this);
  }

  protected <T> CompletableFuture<T> execute(String memberId, BiConsumer<DistributedLogServiceGrpc.DistributedLogServiceStub, StreamObserver<T>> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    callback.accept(factory.getService(memberId), new StreamObserver<T>() {
      @Override
      public void onNext(T response) {
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

  protected <T> CompletableFuture<Void> execute(String memberId, StreamHandler<T> handler, BiConsumer<DistributedLogServiceGrpc.DistributedLogServiceStub, StreamObserver<T>> callback) {
    callback.accept(factory.getService(memberId), new StreamObserver<T>() {
      @Override
      public void onNext(T value) {
        handler.next(value);
      }

      @Override
      public void onError(Throwable t) {
        handler.error(t);
      }

      @Override
      public void onCompleted() {
        handler.complete();
      }
    });
    return CompletableFuture.completedFuture(null);
  }
}
