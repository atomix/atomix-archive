/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.protocol;

import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Test server protocol.
 */
public class TestRaftServerProtocol extends TestRaftProtocol implements RaftServerProtocol {
  private Function<QueryRequest, CompletableFuture<QueryResponse>> queryHandler;
  private BiFunction<QueryRequest, StreamHandler<QueryResponse>, CompletableFuture<Void>> queryStreamHandler;
  private Function<CommandRequest, CompletableFuture<CommandResponse>> commandHandler;
  private BiFunction<CommandRequest, StreamHandler<CommandResponse>, CompletableFuture<Void>> commandStreamHandler;
  private Function<JoinRequest, CompletableFuture<JoinResponse>> joinHandler;
  private Function<LeaveRequest, CompletableFuture<LeaveResponse>> leaveHandler;
  private Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> configureHandler;
  private Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> reconfigureHandler;
  private Function<InstallRequest, CompletableFuture<InstallResponse>> installHandler;
  private Function<TransferRequest, CompletableFuture<TransferResponse>> transferHandler;
  private Function<PollRequest, CompletableFuture<PollResponse>> pollHandler;
  private Function<VoteRequest, CompletableFuture<VoteResponse>> voteHandler;
  private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;

  public TestRaftServerProtocol(String serverId, Map<String, TestRaftServerProtocol> servers, ThreadContext context) {
    super(servers, context);
    servers.put(serverId, this);
  }

  private CompletableFuture<TestRaftServerProtocol> getServer(String serverId) {
    TestRaftServerProtocol server = server(serverId);
    if (server != null) {
      return Futures.completedFuture(server);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(String serverId, QueryRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.query(request)));
  }

  @Override
  public CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler) {
    return getServer(server).thenCompose(listener -> listener.queryStream(request, handler));
  }

  @Override
  public CompletableFuture<CommandResponse> command(String serverId, CommandRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.command(request)));
  }

  @Override
  public CompletableFuture<Void> commandStream(String server, CommandRequest request, StreamHandler<CommandResponse> handler) {
    return getServer(server).thenCompose(listener -> listener.commandStream(request, handler));
  }

  @Override
  public CompletableFuture<JoinResponse> join(String serverId, JoinRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.join(request)));
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(String serverId, LeaveRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.leave(request)));
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(String serverId, ConfigureRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.configure(request)));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(String serverId, ReconfigureRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.reconfigure(request)));
  }

  @Override
  public CompletableFuture<InstallResponse> install(String serverId, InstallRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.install(request)));
  }

  @Override
  public CompletableFuture<TransferResponse> transfer(String serverId, TransferRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.transfer(request)));
  }

  @Override
  public CompletableFuture<PollResponse> poll(String serverId, PollRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.poll(request)));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(String serverId, VoteRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.vote(request)));
  }

  @Override
  public CompletableFuture<AppendResponse> append(String serverId, AppendRequest request) {
    return scheduleTimeout(getServer(serverId).thenCompose(listener -> listener.append(request)));
  }

  CompletableFuture<QueryResponse> query(QueryRequest request) {
    if (queryHandler != null) {
      return queryHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler) {
    this.queryHandler = handler;
  }

  @Override
  public void unregisterQueryHandler() {
    this.queryHandler = null;
  }

  CompletableFuture<Void> queryStream(QueryRequest request, StreamHandler<QueryResponse> handler) {
    if (queryStreamHandler != null) {
      return queryStreamHandler.apply(request, handler);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerQueryStreamHandler(BiFunction<QueryRequest, StreamHandler<QueryResponse>, CompletableFuture<Void>> handler) {
    this.queryStreamHandler = handler;
  }

  @Override
  public void unregisterQueryStreamHandler() {
    this.queryStreamHandler = null;
  }

  CompletableFuture<CommandResponse> command(CommandRequest request) {
    if (commandHandler != null) {
      return commandHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler) {
    this.commandHandler = handler;
  }

  @Override
  public void unregisterCommandHandler() {
    this.commandHandler = null;
  }

  CompletableFuture<Void> commandStream(CommandRequest request, StreamHandler<CommandResponse> handler) {
    if (commandStreamHandler != null) {
      return commandStreamHandler.apply(request, handler);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerCommandStreamHandler(BiFunction<CommandRequest, StreamHandler<CommandResponse>, CompletableFuture<Void>> handler) {
    this.commandStreamHandler = handler;
  }

  @Override
  public void unregisterCommandStreamHandler() {
    this.commandStreamHandler = null;
  }

  CompletableFuture<JoinResponse> join(JoinRequest request) {
    if (joinHandler != null) {
      return joinHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
    this.joinHandler = handler;
  }

  @Override
  public void unregisterJoinHandler() {
    this.joinHandler = null;
  }

  CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    if (leaveHandler != null) {
      return leaveHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
    this.leaveHandler = handler;
  }

  @Override
  public void unregisterLeaveHandler() {
    this.leaveHandler = null;
  }

  CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    if (configureHandler != null) {
      return configureHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
    this.configureHandler = handler;
  }

  @Override
  public void unregisterConfigureHandler() {
    this.configureHandler = null;
  }

  CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
    if (reconfigureHandler != null) {
      return reconfigureHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
    this.reconfigureHandler = handler;
  }

  @Override
  public void unregisterReconfigureHandler() {
    this.reconfigureHandler = null;
  }

  CompletableFuture<InstallResponse> install(InstallRequest request) {
    if (installHandler != null) {
      return installHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
    this.installHandler = handler;
  }

  @Override
  public void unregisterInstallHandler() {
    this.installHandler = null;
  }

  CompletableFuture<TransferResponse> transfer(TransferRequest request) {
    if (transferHandler != null) {
      return transferHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
    this.transferHandler = handler;
  }

  @Override
  public void unregisterTransferHandler() {
    this.transferHandler = null;
  }

  CompletableFuture<PollResponse> poll(PollRequest request) {
    if (pollHandler != null) {
      return pollHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
    this.pollHandler = handler;
  }

  @Override
  public void unregisterPollHandler() {
    this.pollHandler = null;
  }

  CompletableFuture<VoteResponse> vote(VoteRequest request) {
    if (voteHandler != null) {
      return voteHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
  }

  @Override
  public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
    this.voteHandler = handler;
  }

  @Override
  public void unregisterVoteHandler() {
    this.voteHandler = null;
  }

  CompletableFuture<AppendResponse> append(AppendRequest request) {
    if (appendHandler != null) {
      return appendHandler.apply(request);
    } else {
      return Futures.exceptionalFuture(new ConnectException());
    }
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
