package io.atomix.protocols.raft.protocol.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftServiceGrpc;
import io.atomix.server.management.ServiceFactory;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.stub.StreamObserver;

/**
 * gRPC client protocol.
 */
public class GrpcClientProtocol implements RaftClientProtocol {
  private final ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory;

  public GrpcClientProtocol(ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory) {
    this.factory = factory;
  }

  @Override
  public CompletableFuture<QueryResponse> query(String server, QueryRequest request) {
    return execute(server, (raft, observer) -> raft.query(request, observer));
  }

  @Override
  public CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler) {
    return execute(server, handler, (raft, observer) -> raft.queryStream(request, observer));
  }

  @Override
  public CompletableFuture<CommandResponse> command(String server, CommandRequest request) {
    return execute(server, (raft, observer) -> raft.command(request, observer));
  }

  @Override
  public CompletableFuture<Void> commandStream(String server, CommandRequest request, StreamHandler<CommandResponse> handler) {
    return execute(server, handler, (raft, observer) -> raft.commandStream(request, observer));
  }

  protected <T> CompletableFuture<T> execute(String memberId, BiConsumer<RaftServiceGrpc.RaftServiceStub, StreamObserver<T>> callback) {
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

  protected <T> CompletableFuture<Void> execute(String memberId, StreamHandler<T> handler, BiConsumer<RaftServiceGrpc.RaftServiceStub, StreamObserver<T>> callback) {
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
