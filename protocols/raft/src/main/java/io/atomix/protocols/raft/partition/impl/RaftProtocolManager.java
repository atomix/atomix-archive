package io.atomix.protocols.raft.partition.impl;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.grpc.ServiceFactory;
import io.atomix.cluster.grpc.ServiceProvider;
import io.atomix.cluster.grpc.ServiceRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.protocols.raft.RaftPartitionAppend;
import io.atomix.protocols.raft.RaftPartitionCommand;
import io.atomix.protocols.raft.RaftPartitionConfigure;
import io.atomix.protocols.raft.RaftPartitionInstall;
import io.atomix.protocols.raft.RaftPartitionJoin;
import io.atomix.protocols.raft.RaftPartitionLeave;
import io.atomix.protocols.raft.RaftPartitionPoll;
import io.atomix.protocols.raft.RaftPartitionQuery;
import io.atomix.protocols.raft.RaftPartitionReconfigure;
import io.atomix.protocols.raft.RaftPartitionServiceGrpc;
import io.atomix.protocols.raft.RaftPartitionTransfer;
import io.atomix.protocols.raft.RaftPartitionVote;
import io.atomix.raft.protocol.AppendRequest;
import io.atomix.raft.protocol.AppendResponse;
import io.atomix.raft.protocol.CommandRequest;
import io.atomix.raft.protocol.CommandResponse;
import io.atomix.raft.protocol.ConfigureRequest;
import io.atomix.raft.protocol.ConfigureResponse;
import io.atomix.raft.protocol.InstallRequest;
import io.atomix.raft.protocol.InstallResponse;
import io.atomix.raft.protocol.JoinRequest;
import io.atomix.raft.protocol.JoinResponse;
import io.atomix.raft.protocol.LeaveRequest;
import io.atomix.raft.protocol.LeaveResponse;
import io.atomix.raft.protocol.PollRequest;
import io.atomix.raft.protocol.PollResponse;
import io.atomix.raft.protocol.QueryRequest;
import io.atomix.raft.protocol.QueryResponse;
import io.atomix.raft.protocol.RaftClientProtocol;
import io.atomix.raft.protocol.RaftServerProtocol;
import io.atomix.raft.protocol.ReconfigureRequest;
import io.atomix.raft.protocol.ReconfigureResponse;
import io.atomix.raft.protocol.TransferRequest;
import io.atomix.raft.protocol.TransferResponse;
import io.atomix.raft.protocol.VoteRequest;
import io.atomix.raft.protocol.VoteResponse;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.stub.StreamObserver;

/**
 * Raft protocol manager.
 */
@Component
public class RaftProtocolManager extends RaftPartitionServiceGrpc.RaftPartitionServiceImplBase implements Managed {
  private static final ConnectException CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  @Dependency
  private ServiceProvider serviceProvider;
  @Dependency
  private ServiceRegistry serviceRegistry;

  private ServiceFactory<RaftPartitionServiceGrpc.RaftPartitionServiceStub> factory;
  private final Map<PartitionId, RaftClientProtocolImpl> clients = new ConcurrentHashMap<>();
  private final Map<PartitionId, RaftServerProtocolImpl> servers = new ConcurrentHashMap<>();

  /**
   * Returns the client protocol for the given partition.
   *
   * @param partitionId the partition ID
   * @return the client protocol for the given partition
   */
  public RaftClientProtocol getClientProtocol(PartitionId partitionId) {
    RaftClientProtocol protocol = clients.get(partitionId);
    return protocol != null ? protocol : clients.computeIfAbsent(partitionId, RaftClientProtocolImpl::new);
  }

  /**
   * Returns the server protocol for the given partition.
   *
   * @param partitionId the partition ID
   * @return the server protocol for the given partition
   */
  public RaftServerProtocol getServerProtocol(PartitionId partitionId) {
    RaftServerProtocol protocol = servers.get(partitionId);
    return protocol != null ? protocol : servers.computeIfAbsent(partitionId, RaftServerProtocolImpl::new);
  }

  @Override
  public CompletableFuture<Void> start() {
    factory = serviceProvider.getFactory(RaftPartitionServiceGrpc::newStub);
    return CompletableFuture.completedFuture(null);
  }

  private <T> void handle(
      PartitionId partitionId,
      Predicate<RaftServerProtocolImpl> predicate,
      Function<RaftServerProtocolImpl, CompletableFuture<T>> handler,
      StreamObserver<T> responseObserver) {
    RaftServerProtocolImpl server = servers.get(partitionId);
    if (server != null && predicate.test(server)) {
      handler.apply(server).whenComplete((response, error) -> {
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

  private <T> void handle(
      PartitionId partitionId,
      Predicate<RaftServerProtocolImpl> predicate,
      BiConsumer<RaftServerProtocolImpl, StreamHandler<T>> handler,
      StreamObserver<T> responseObserver) {
    RaftServerProtocolImpl server = servers.get(partitionId);
    if (server != null && predicate.test(server)) {
      handler.accept(server, new StreamHandler<T>() {
        @Override
        public void next(T value) {
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
      });
    } else {
      responseObserver.onError(CONNECT_EXCEPTION);
    }
  }

  @Override
  public void join(RaftPartitionJoin request, StreamObserver<JoinResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.joinHandler != null,
        server -> server.joinHandler.apply(request.getJoin()),
        responseObserver);
  }

  @Override
  public void leave(RaftPartitionLeave request, StreamObserver<LeaveResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.leaveHandler != null,
        server -> server.leaveHandler.apply(request.getLeave()),
        responseObserver);
  }

  @Override
  public void configure(RaftPartitionConfigure request, StreamObserver<ConfigureResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.configureHandler != null,
        server -> server.configureHandler.apply(request.getConfigure()),
        responseObserver);
  }

  @Override
  public void reconfigure(RaftPartitionReconfigure request, StreamObserver<ReconfigureResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.reconfigureHandler != null,
        server -> server.reconfigureHandler.apply(request.getReconfigure()),
        responseObserver);
  }

  @Override
  public void poll(RaftPartitionPoll request, StreamObserver<PollResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.pollHandler != null,
        server -> server.pollHandler.apply(request.getPoll()),
        responseObserver);
  }

  @Override
  public void vote(RaftPartitionVote request, StreamObserver<VoteResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.voteHandler != null,
        server -> server.voteHandler.apply(request.getVote()),
        responseObserver);
  }

  @Override
  public void transfer(RaftPartitionTransfer request, StreamObserver<TransferResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.transferHandler != null,
        server -> server.transferHandler.apply(request.getTransfer()),
        responseObserver);
  }

  @Override
  public void append(RaftPartitionAppend request, StreamObserver<AppendResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.appendHandler != null,
        server -> server.appendHandler.apply(request.getAppend()),
        responseObserver);
  }

  @Override
  public void install(RaftPartitionInstall request, StreamObserver<InstallResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.installHandler != null,
        server -> server.installHandler.apply(request.getInstall()),
        responseObserver);
  }

  @Override
  public void command(RaftPartitionCommand request, StreamObserver<CommandResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.commandHandler != null,
        server -> server.commandHandler.apply(request.getCommand()),
        responseObserver);
  }

  @Override
  public void commandStream(RaftPartitionCommand request, StreamObserver<CommandResponse> responseObserver) {
    this.handle(
        request.getId(),
        server -> server.commandStreamHandler != null,
        (server, stream) -> server.commandStreamHandler.apply(request.getCommand(), stream),
        responseObserver);
  }

  @Override
  public void query(RaftPartitionQuery request, StreamObserver<QueryResponse> responseObserver) {
    handle(
        request.getId(),
        server -> server.queryHandler != null,
        server -> server.queryHandler.apply(request.getQuery()),
        responseObserver);
  }

  @Override
  public void queryStream(RaftPartitionQuery request, StreamObserver<QueryResponse> responseObserver) {
    this.handle(
        request.getId(),
        server -> server.queryStreamHandler != null,
        (server, stream) -> server.queryStreamHandler.apply(request.getQuery(), stream),
        responseObserver);
  }

  private abstract class RaftProtocolBase {
    <T> CompletableFuture<T> execute(String memberId, BiConsumer<RaftPartitionServiceGrpc.RaftPartitionServiceStub, StreamObserver<T>> callback) {
      CompletableFuture<T> future = new CompletableFuture<>();
      callback.accept(factory.getService(MemberId.from(memberId)), new StreamObserver<T>() {
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

    <T> CompletableFuture<Void> execute(String memberId, StreamHandler<T> handler, BiConsumer<RaftPartitionServiceGrpc.RaftPartitionServiceStub, StreamObserver<T>> callback) {
      callback.accept(factory.getService(MemberId.from(memberId)), new StreamObserver<T>() {
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

  private class RaftClientProtocolImpl extends RaftProtocolBase implements RaftClientProtocol {
    protected final PartitionId partitionId;

    RaftClientProtocolImpl(PartitionId partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public CompletableFuture<QueryResponse> query(String server, QueryRequest request) {
      return execute(server, (raft, observer) -> raft.query(RaftPartitionQuery.newBuilder()
          .setId(partitionId)
          .setQuery(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler) {
      return execute(server, handler, (raft, observer) -> raft.queryStream(RaftPartitionQuery.newBuilder()
          .setId(partitionId)
          .setQuery(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<CommandResponse> command(String server, CommandRequest request) {
      return execute(server, (raft, observer) -> raft.command(RaftPartitionCommand.newBuilder()
          .setId(partitionId)
          .setCommand(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<Void> commandStream(String server, CommandRequest request, StreamHandler<CommandResponse> handler) {
      return execute(server, handler, (raft, observer) -> raft.commandStream(RaftPartitionCommand.newBuilder()
          .setId(partitionId)
          .setCommand(request)
          .build(), observer));
    }
  }

  private class RaftServerProtocolImpl extends RaftClientProtocolImpl implements RaftServerProtocol {
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

    RaftServerProtocolImpl(PartitionId partitionId) {
      super(partitionId);
    }

    @Override
    public CompletableFuture<JoinResponse> join(String server, JoinRequest request) {
      return execute(server, (raft, observer) -> raft.join(RaftPartitionJoin.newBuilder()
          .setId(partitionId)
          .setJoin(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(String server, LeaveRequest request) {
      return execute(server, (raft, observer) -> raft.leave(RaftPartitionLeave.newBuilder()
          .setId(partitionId)
          .setLeave(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<ConfigureResponse> configure(String server, ConfigureRequest request) {
      return execute(server, (raft, observer) -> raft.configure(RaftPartitionConfigure.newBuilder()
          .setId(partitionId)
          .setConfigure(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<ReconfigureResponse> reconfigure(String server, ReconfigureRequest request) {
      return execute(server, (raft, observer) -> raft.reconfigure(RaftPartitionReconfigure.newBuilder()
          .setId(partitionId)
          .setReconfigure(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<InstallResponse> install(String server, InstallRequest request) {
      return execute(server, (raft, observer) -> raft.install(RaftPartitionInstall.newBuilder()
          .setId(partitionId)
          .setInstall(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<TransferResponse> transfer(String server, TransferRequest request) {
      return execute(server, (raft, observer) -> raft.transfer(RaftPartitionTransfer.newBuilder()
          .setId(partitionId)
          .setTransfer(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<PollResponse> poll(String server, PollRequest request) {
      return execute(server, (raft, observer) -> raft.poll(RaftPartitionPoll.newBuilder()
          .setId(partitionId)
          .setPoll(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<VoteResponse> vote(String server, VoteRequest request) {
      return execute(server, (raft, observer) -> raft.vote(RaftPartitionVote.newBuilder()
          .setId(partitionId)
          .setVote(request)
          .build(), observer));
    }

    @Override
    public CompletableFuture<AppendResponse> append(String server, AppendRequest request) {
      return execute(server, (raft, observer) -> raft.append(RaftPartitionAppend.newBuilder()
          .setId(partitionId)
          .setAppend(request)
          .build(), observer));
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
  }
}
