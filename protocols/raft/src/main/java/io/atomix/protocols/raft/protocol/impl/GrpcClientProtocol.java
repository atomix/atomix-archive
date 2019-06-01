package io.atomix.protocols.raft.protocol.impl;

import java.net.ConnectException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftServiceGrpc;
import io.atomix.server.NodeConfig;
import io.atomix.server.management.ServiceFactory;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.stub.StreamObserver;

/**
 * gRPC client protocol.
 */
public class GrpcClientProtocol implements RaftClientProtocol {
  private static final ConnectException CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private final ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory;
  private final Map<String, NodeConfig> members = new ConcurrentHashMap<>();

  public GrpcClientProtocol(ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory, Collection<NodeConfig> members) {
    this.factory = factory;
    members.forEach(member -> this.members.put(member.getId(), member));
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
    NodeConfig member = members.get(memberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }

    CompletableFuture<T> future = new CompletableFuture<>();
    callback.accept(factory.getService(member.getHost(), member.getPort()), new StreamObserver<T>() {
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
    NodeConfig member = members.get(memberId);
    if (member == null) {
      return Futures.exceptionalFuture(CONNECT_EXCEPTION);
    }

    callback.accept(factory.getService(member.getHost(), member.getPort()), new StreamObserver<T>() {
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
