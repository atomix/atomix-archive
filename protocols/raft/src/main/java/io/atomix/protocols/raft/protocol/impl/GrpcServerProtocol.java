package io.atomix.protocols.raft.protocol.impl;

import java.net.ConnectException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.JoinResponse;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.LeaveResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.protocol.RaftServiceGrpc;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.TransferRequest;
import io.atomix.protocols.raft.protocol.TransferResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;
import io.atomix.server.NodeConfig;
import io.atomix.server.management.ServiceFactory;
import io.atomix.server.management.ServiceRegistry;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.stub.StreamObserver;

/**
 * gRPC server protocol.
 */
public class GrpcServerProtocol extends RaftServiceGrpc.RaftServiceImplBase implements RaftServerProtocol {
  private static final ConnectException CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private final ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory;
  private final Map<String, NodeConfig> members = new ConcurrentHashMap<>();
  private Function<QueryRequest, CompletableFuture<QueryResponse>> queryHandler;
  private BiFunction<QueryRequest, StreamHandler<QueryResponse>, CompletableFuture<Void>> queryStreamHandler;
  private Function<CommandRequest, CompletableFuture<CommandResponse>> commandHandler;
  private BiFunction<CommandRequest, StreamHandler<CommandResponse>, CompletableFuture<Void>> commandStreamHandler;
  private Function<JoinRequest, CompletableFuture<JoinResponse>> joinHandler;
  private Function<LeaveRequest, CompletableFuture<LeaveResponse>> leaveHandler;
  private Function<TransferRequest, CompletableFuture<TransferResponse>> transferHandler;
  private Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> configureHandler;
  private Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> reconfigureHandler;
  private Function<InstallRequest, CompletableFuture<InstallResponse>> installHandler;
  private Function<PollRequest, CompletableFuture<PollResponse>> pollHandler;
  private Function<VoteRequest, CompletableFuture<VoteResponse>> voteHandler;
  private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;

  public GrpcServerProtocol(
      ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory,
      ServiceRegistry registry,
      Collection<NodeConfig> members) {
    this.factory = factory;
    members.forEach(member -> this.members.put(member.getId(), member));
    registry.register(this);
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
  public void join(JoinRequest request, StreamObserver<JoinResponse> responseObserver) {
    handle(request, joinHandler, responseObserver);
  }

  @Override
  public void leave(LeaveRequest request, StreamObserver<LeaveResponse> responseObserver) {
    handle(request, leaveHandler, responseObserver);
  }

  @Override
  public void configure(ConfigureRequest request, StreamObserver<ConfigureResponse> responseObserver) {
    handle(request, configureHandler, responseObserver);
  }

  @Override
  public void reconfigure(ReconfigureRequest request, StreamObserver<ReconfigureResponse> responseObserver) {
    handle(request, reconfigureHandler, responseObserver);
  }

  @Override
  public void poll(PollRequest request, StreamObserver<PollResponse> responseObserver) {
    handle(request, pollHandler, responseObserver);
  }

  @Override
  public void vote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
    handle(request, voteHandler, responseObserver);
  }

  @Override
  public void transfer(TransferRequest request, StreamObserver<TransferResponse> responseObserver) {
    handle(request, transferHandler, responseObserver);
  }

  @Override
  public void append(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
    handle(request, appendHandler, responseObserver);
  }

  @Override
  public void install(InstallRequest request, StreamObserver<InstallResponse> responseObserver) {
    handle(request, installHandler, responseObserver);
  }

  @Override
  public void command(CommandRequest request, StreamObserver<CommandResponse> responseObserver) {
    handle(request, commandHandler, responseObserver);
  }

  @Override
  public void commandStream(CommandRequest request, StreamObserver<CommandResponse> responseObserver) {
    handle(request, commandStreamHandler, responseObserver);
  }

  @Override
  public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
    handle(request, queryHandler, responseObserver);
  }

  @Override
  public void queryStream(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
    handle(request, queryStreamHandler, responseObserver);
  }

  @Override
  public CompletableFuture<JoinResponse> join(String server, JoinRequest request) {
    return execute(server, (raft, observer) -> raft.join(request, observer));
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(String server, LeaveRequest request) {
    return execute(server, (raft, observer) -> raft.leave(request, observer));
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(String server, ConfigureRequest request) {
    return execute(server, (raft, observer) -> raft.configure(request, observer));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(String server, ReconfigureRequest request) {
    return execute(server, (raft, observer) -> raft.reconfigure(request, observer));
  }

  @Override
  public CompletableFuture<InstallResponse> install(String server, InstallRequest request) {
    return execute(server, (raft, observer) -> raft.install(request, observer));
  }

  @Override
  public CompletableFuture<TransferResponse> transfer(String server, TransferRequest request) {
    return execute(server, (raft, observer) -> raft.transfer(request, observer));
  }

  @Override
  public CompletableFuture<PollResponse> poll(String server, PollRequest request) {
    return execute(server, (raft, observer) -> raft.poll(request, observer));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(String server, VoteRequest request) {
    return execute(server, (raft, observer) -> raft.vote(request, observer));
  }

  @Override
  public CompletableFuture<AppendResponse> append(String server, AppendRequest request) {
    return execute(server, (raft, observer) -> raft.append(request, observer));
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

  @Override
  public void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler) {
    this.queryHandler = handler;
  }

  @Override
  public void unregisterQueryHandler() {
    this.queryHandler = null;
  }

  @Override
  public void registerQueryStreamHandler(BiFunction<QueryRequest, StreamHandler<QueryResponse>, CompletableFuture<Void>> handler) {
    this.queryStreamHandler = handler;
  }

  @Override
  public void unregisterQueryStreamHandler() {
    this.queryStreamHandler = null;
  }

  @Override
  public void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler) {
    this.commandHandler = handler;
  }

  @Override
  public void unregisterCommandHandler() {
    this.commandHandler = null;
  }

  @Override
  public void registerCommandStreamHandler(BiFunction<CommandRequest, StreamHandler<CommandResponse>, CompletableFuture<Void>> handler) {
    this.commandStreamHandler = handler;
  }

  @Override
  public void unregisterCommandStreamHandler() {
    this.commandStreamHandler = null;
  }

  @Override
  public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
    this.joinHandler = handler;
  }

  @Override
  public void unregisterJoinHandler() {
    this.joinHandler = null;
  }

  @Override
  public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
    this.leaveHandler = handler;
  }

  @Override
  public void unregisterLeaveHandler() {
    this.leaveHandler = null;
  }

  @Override
  public void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
    this.transferHandler = handler;
  }

  @Override
  public void unregisterTransferHandler() {
    this.transferHandler = null;
  }

  @Override
  public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
    this.configureHandler = handler;
  }

  @Override
  public void unregisterConfigureHandler() {
    this.configureHandler = null;
  }

  @Override
  public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
    this.reconfigureHandler = handler;
  }

  @Override
  public void unregisterReconfigureHandler() {
    this.reconfigureHandler = null;
  }

  @Override
  public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
    this.installHandler = handler;
  }

  @Override
  public void unregisterInstallHandler() {
    this.installHandler = null;
  }

  @Override
  public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
    this.pollHandler = handler;
  }

  @Override
  public void unregisterPollHandler() {
    this.pollHandler = null;
  }

  @Override
  public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
    this.voteHandler = handler;
  }

  @Override
  public void unregisterVoteHandler() {
    this.voteHandler = null;
  }

  @Override
  public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    this.appendHandler = handler;
  }

  @Override
  public void unregisterAppendHandler() {
    this.appendHandler = null;
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
