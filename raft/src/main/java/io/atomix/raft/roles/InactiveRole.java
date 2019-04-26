/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.roles;

import java.util.concurrent.CompletableFuture;

import io.atomix.raft.RaftServer;
import io.atomix.raft.impl.RaftContext;
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
import io.atomix.raft.protocol.RaftError;
import io.atomix.raft.protocol.ReconfigureRequest;
import io.atomix.raft.protocol.ReconfigureResponse;
import io.atomix.raft.protocol.ResponseStatus;
import io.atomix.raft.protocol.TransferRequest;
import io.atomix.raft.protocol.TransferResponse;
import io.atomix.raft.protocol.VoteRequest;
import io.atomix.raft.protocol.VoteResponse;
import io.atomix.raft.storage.system.RaftConfiguration;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.Futures;

/**
 * Inactive state.
 */
public class InactiveRole extends AbstractRole {

  public InactiveRole(RaftContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.INACTIVE;
  }

  @Override
  public CompletableFuture<ConfigureResponse> onConfigure(ConfigureRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.getTerm(), request.getLeader());

    RaftConfiguration configuration = RaftConfiguration.newBuilder()
        .setIndex(request.getIndex())
        .setTerm(request.getTerm())
        .setTimestamp(request.getTimestamp())
        .addAllMembers(request.getMembersList())
        .build();

    // Configure the cluster membership. This will cause this server to transition to the
    // appropriate state if its type has changed.
    raft.getCluster().configure(configuration);

    // If the configuration is already committed, commit it to disk.
    // Check against the actual cluster Configuration rather than the received configuration in
    // case the received configuration was an older configuration that was not applied.
    if (raft.getCommitIndex() >= raft.getCluster().getConfiguration().getIndex()) {
      raft.getCluster().commit();
    }

    return CompletableFuture.completedFuture(logResponse(ConfigureResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .build()));
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(InstallResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(JoinRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(JoinResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(ReconfigureResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(LeaveResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<TransferResponse> onTransfer(TransferRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(TransferResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(AppendRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(AppendResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(PollResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(VoteResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(CommandResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<Void> onCommand(CommandRequest request, StreamHandler<CommandResponse> handler) {
    logRequest(request);
    handler.next(logResponse(CommandResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
    handler.complete();
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
    logRequest(request);
    return Futures.completedFuture(logResponse(QueryResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
  }

  @Override
  public CompletableFuture<Void> onQuery(QueryRequest request, StreamHandler<QueryResponse> handler) {
    logRequest(request);
    handler.next(logResponse(QueryResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.UNAVAILABLE)
        .build()));
    return CompletableFuture.completedFuture(null);
  }
}
