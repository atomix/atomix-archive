package io.atomix.protocols.log.protocol.impl;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.DistributedLogServiceGrpc;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.LogServerProtocol;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.protocols.log.protocol.ResetResponse;
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
  private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;
  private BiFunction<ConsumeRequest, StreamHandler<ConsumeResponse>, CompletableFuture<Void>> consumeHandler;
  private Consumer<ResetRequest> resetHandler;
  private Function<BackupRequest, CompletableFuture<BackupResponse>> backupHandler;

  public GrpcProtocol(ServiceFactory<DistributedLogServiceGrpc.DistributedLogServiceStub> factory, ServiceRegistry registry) {
    this.factory = factory;
    registry.register(this);
  }

  @Override
  public void append(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
    handle(request, appendHandler, responseObserver);
  }

  @Override
  public void consume(ConsumeRequest request, StreamObserver<ConsumeResponse> responseObserver) {
    handle(request, consumeHandler, responseObserver);
  }

  @Override
  public void backup(BackupRequest request, StreamObserver<BackupResponse> responseObserver) {
    handle(request, backupHandler, responseObserver);
  }

  @Override
  public void reset(ResetRequest request, StreamObserver<ResetResponse> responseObserver) {
    handle(request, r -> {
      resetHandler.accept(r);
      return CompletableFuture.completedFuture(ResetResponse.newBuilder().build());
    }, responseObserver);
  }

  private <T, R> void handle(
      T request,
      Function<T, CompletableFuture<R>> handler,
      StreamObserver<R> responseObserver) {
    if (handler != null) {
      handler.apply(request).whenComplete((response, error) -> {
        if (error == null) {
          responseObserver.onNext(response);
          responseObserver.onCompleted();
        } else {
          responseObserver.onError(error);
        }
      });
    } else {
      responseObserver.onError(CONNECT_EXCEPTION);
    }
  }

  private <T, R> void handle(
      T request,
      BiFunction<T, StreamHandler<R>, CompletableFuture<Void>> handler,
      StreamObserver<R> responseObserver) {
    if (handler != null) {
      handler.apply(request, new StreamHandler<R>() {
        @Override
        public void next(R value) {
          responseObserver.onNext(value);
        }

        @Override
        public void complete() {
          responseObserver.onCompleted();
        }

        @Override
        public void error(Throwable error) {
          responseObserver.onError(error);
        }
      }).whenComplete((result, error) -> {
        if (error != null) {
          responseObserver.onError(error);
        }
      });
    } else {
      responseObserver.onError(CONNECT_EXCEPTION);
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(String memberId, AppendRequest request) {
    return execute(memberId, (log, stream) -> log.append(request, stream));
  }

  @Override
  public CompletableFuture<Void> consume(String memberId, ConsumeRequest request, StreamHandler<ConsumeResponse> handler) {
    return execute(memberId, handler, (log, stream) -> log.consume(request, stream));
  }

  @Override
  public void reset(String memberId, ResetRequest request) {
    this.<ResetResponse>execute(memberId, (log, stream) -> log.reset(request, stream));
  }

  @Override
  public CompletableFuture<BackupResponse> backup(String memberId, BackupRequest request) {
    return execute(memberId, (log, stream) -> log.backup(request, stream));
  }

  @Override
  public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    this.appendHandler = handler;
  }

  @Override
  public void unregisterAppendHandler() {
    this.appendHandler = null;
  }

  @Override
  public void registerConsumeHandler(BiFunction<ConsumeRequest, StreamHandler<ConsumeResponse>, CompletableFuture<Void>> handler) {
    this.consumeHandler = handler;
  }

  @Override
  public void unregisterConsumeHandler() {
    this.consumeHandler = null;
  }

  @Override
  public void registerResetConsumer(Consumer<ResetRequest> consumer, Executor executor) {
    this.resetHandler = request -> executor.execute(() -> consumer.accept(request));
  }

  @Override
  public void unregisterResetConsumer() {
    this.resetHandler = null;
  }

  @Override
  public void registerBackupHandler(Function<BackupRequest, CompletableFuture<BackupResponse>> handler) {
    this.backupHandler = handler;
  }

  @Override
  public void unregisterBackupHandler() {
    this.backupHandler = null;
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
