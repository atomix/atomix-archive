/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.protocols.raft.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.protocol.AppendRequest;
import io.atomix.protocols.raft.protocol.AppendResponse;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.ConfigureRequest;
import io.atomix.protocols.raft.protocol.ConfigureResponse;
import io.atomix.protocols.raft.protocol.InstallRequest;
import io.atomix.protocols.raft.protocol.InstallResponse;
import io.atomix.protocols.raft.protocol.JoinRequest;
import io.atomix.protocols.raft.protocol.JoinResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.LeaveRequest;
import io.atomix.protocols.raft.protocol.LeaveResponse;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.OperationRequest;
import io.atomix.protocols.raft.protocol.OperationResponse;
import io.atomix.protocols.raft.protocol.PollRequest;
import io.atomix.protocols.raft.protocol.PollResponse;
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.RaftServerProtocol;
import io.atomix.protocols.raft.protocol.ReconfigureRequest;
import io.atomix.protocols.raft.protocol.ReconfigureResponse;
import io.atomix.protocols.raft.protocol.ResetRequest;
import io.atomix.protocols.raft.protocol.TransferRequest;
import io.atomix.protocols.raft.protocol.TransferResponse;
import io.atomix.protocols.raft.protocol.VoteRequest;
import io.atomix.protocols.raft.protocol.VoteResponse;

import static io.atomix.utils.concurrent.Futures.uncheck;

/**
 * Raft server protocol that uses a {@link ClusterCommunicationService}.
 */
public class RaftServerCommunicator implements RaftServerProtocol {
  private final RaftMessageContext context;
  private final ClusterCommunicationService clusterCommunicator;

  public RaftServerCommunicator(ClusterCommunicationService clusterCommunicator) {
    this(null, clusterCommunicator);
  }

  public RaftServerCommunicator(String prefix, ClusterCommunicationService clusterCommunicator) {
    this.context = new RaftMessageContext(prefix);
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(
      String subject, T request, Function<T, byte[]> encoder, Function<byte[], U> decoder, MemberId memberId) {
    return clusterCommunicator.send(subject, request, encoder, decoder, MemberId.from(memberId.id()));
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
    return sendAndReceive(
        context.openSessionSubject,
        request,
        OpenSessionRequest::toByteArray,
        uncheck(OpenSessionResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(MemberId memberId, CloseSessionRequest request) {
    return sendAndReceive(
        context.closeSessionSubject,
        request,
        CloseSessionRequest::toByteArray,
        uncheck(CloseSessionResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(MemberId memberId, KeepAliveRequest request) {
    return sendAndReceive(
        context.keepAliveSubject,
        request,
        KeepAliveRequest::toByteArray,
        uncheck(KeepAliveResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<OperationResponse> query(MemberId memberId, OperationRequest request) {
    return sendAndReceive(
        context.querySubject,
        request,
        OperationRequest::toByteArray,
        uncheck(OperationResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<OperationResponse> command(MemberId memberId, OperationRequest request) {
    return sendAndReceive(
        context.commandSubject,
        request,
        OperationRequest::toByteArray,
        uncheck(OperationResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
    return sendAndReceive(
        context.metadataSubject,
        request,
        MetadataRequest::toByteArray,
        uncheck(MetadataResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<JoinResponse> join(MemberId memberId, JoinRequest request) {
    return sendAndReceive(
        context.joinSubject,
        request,
        JoinRequest::toByteArray,
        uncheck(JoinResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(MemberId memberId, LeaveRequest request) {
    return sendAndReceive(
        context.leaveSubject,
        request,
        LeaveRequest::toByteArray,
        uncheck(LeaveResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(MemberId memberId, ConfigureRequest request) {
    return sendAndReceive(
        context.configureSubject,
        request,
        ConfigureRequest::toByteArray,
        uncheck(ConfigureResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(MemberId memberId, ReconfigureRequest request) {
    return sendAndReceive(
        context.reconfigureSubject,
        request,
        ReconfigureRequest::toByteArray,
        uncheck(ReconfigureResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<InstallResponse> install(MemberId memberId, InstallRequest request) {
    return sendAndReceive(
        context.installSubject,
        request,
        InstallRequest::toByteArray,
        uncheck(InstallResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<TransferResponse> transfer(MemberId memberId, TransferRequest request) {
    return sendAndReceive(
        context.transferSubject,
        request,
        TransferRequest::toByteArray,
        uncheck(TransferResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<PollResponse> poll(MemberId memberId, PollRequest request) {
    return sendAndReceive(
        context.pollSubject,
        request,
        PollRequest::toByteArray,
        uncheck(PollResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(MemberId memberId, VoteRequest request) {
    return sendAndReceive(
        context.voteSubject,
        request,
        VoteRequest::toByteArray,
        uncheck(VoteResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
    return sendAndReceive(
        context.appendSubject,
        request,
        AppendRequest::toByteArray,
        uncheck(AppendResponse::parseFrom),
        memberId);
  }

  @Override
  public void publish(MemberId memberId, PublishRequest request) {
    clusterCommunicator.unicast(
        context.publishSubject(request.getSessionId()),
        request,
        PublishRequest::toByteArray,
        memberId);
  }

  @Override
  public void registerOpenSessionHandler(Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> handler) {
    clusterCommunicator.subscribe(
        context.openSessionSubject,
        uncheck(OpenSessionRequest::parseFrom),
        handler,
        OpenSessionResponse::toByteArray);
  }

  @Override
  public void unregisterOpenSessionHandler() {
    clusterCommunicator.unsubscribe(context.openSessionSubject);
  }

  @Override
  public void registerCloseSessionHandler(Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> handler) {
    clusterCommunicator.subscribe(
        context.closeSessionSubject,
        uncheck(CloseSessionRequest::parseFrom),
        handler,
        CloseSessionResponse::toByteArray);
  }

  @Override
  public void unregisterCloseSessionHandler() {
    clusterCommunicator.unsubscribe(context.closeSessionSubject);
  }

  @Override
  public void registerKeepAliveHandler(Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> handler) {
    clusterCommunicator.subscribe(
        context.keepAliveSubject,
        uncheck(KeepAliveRequest::parseFrom),
        handler,
        KeepAliveResponse::toByteArray);
  }

  @Override
  public void unregisterKeepAliveHandler() {
    clusterCommunicator.unsubscribe(context.keepAliveSubject);
  }

  @Override
  public void registerQueryHandler(Function<OperationRequest, CompletableFuture<OperationResponse>> handler) {
    clusterCommunicator.subscribe(
        context.querySubject,
        uncheck(OperationRequest::parseFrom),
        handler,
        OperationResponse::toByteArray);
  }

  @Override
  public void unregisterQueryHandler() {
    clusterCommunicator.unsubscribe(context.querySubject);
  }

  @Override
  public void registerCommandHandler(Function<OperationRequest, CompletableFuture<OperationResponse>> handler) {
    clusterCommunicator.subscribe(
        context.commandSubject,
        uncheck(OperationRequest::parseFrom),
        handler,
        OperationResponse::toByteArray);
  }

  @Override
  public void unregisterCommandHandler() {
    clusterCommunicator.unsubscribe(context.commandSubject);
  }

  @Override
  public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
    clusterCommunicator.subscribe(
        context.metadataSubject,
        uncheck(MetadataRequest::parseFrom),
        handler,
        MetadataResponse::toByteArray);
  }

  @Override
  public void unregisterMetadataHandler() {
    clusterCommunicator.unsubscribe(context.metadataSubject);
  }

  @Override
  public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
    clusterCommunicator.subscribe(
        context.joinSubject,
        uncheck(JoinRequest::parseFrom),
        handler,
        JoinResponse::toByteArray);
  }

  @Override
  public void unregisterJoinHandler() {
    clusterCommunicator.unsubscribe(context.joinSubject);
  }

  @Override
  public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
    clusterCommunicator.subscribe(
        context.leaveSubject,
        uncheck(LeaveRequest::parseFrom),
        handler,
        LeaveResponse::toByteArray);
  }

  @Override
  public void unregisterLeaveHandler() {
    clusterCommunicator.unsubscribe(context.leaveSubject);
  }

  @Override
  public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
    clusterCommunicator.subscribe(
        context.configureSubject,
        uncheck(ConfigureRequest::parseFrom),
        handler,
        ConfigureResponse::toByteArray);
  }

  @Override
  public void unregisterConfigureHandler() {
    clusterCommunicator.unsubscribe(context.configureSubject);
  }

  @Override
  public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
    clusterCommunicator.subscribe(
        context.reconfigureSubject,
        uncheck(ReconfigureRequest::parseFrom),
        handler,
        ReconfigureResponse::toByteArray);
  }

  @Override
  public void unregisterReconfigureHandler() {
    clusterCommunicator.unsubscribe(context.reconfigureSubject);
  }

  @Override
  public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
    clusterCommunicator.subscribe(
        context.installSubject,
        uncheck(InstallRequest::parseFrom),
        handler,
        InstallResponse::toByteArray);
  }

  @Override
  public void unregisterInstallHandler() {
    clusterCommunicator.unsubscribe(context.installSubject);
  }

  @Override
  public void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
    clusterCommunicator.subscribe(
        context.transferSubject,
        uncheck(TransferRequest::parseFrom),
        handler,
        TransferResponse::toByteArray);
  }

  @Override
  public void unregisterTransferHandler() {
    clusterCommunicator.unsubscribe(context.transferSubject);
  }

  @Override
  public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
    clusterCommunicator.subscribe(
        context.pollSubject,
        uncheck(PollRequest::parseFrom),
        handler,
        PollResponse::toByteArray);
  }

  @Override
  public void unregisterPollHandler() {
    clusterCommunicator.unsubscribe(context.pollSubject);
  }

  @Override
  public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
    clusterCommunicator.subscribe(
        context.voteSubject,
        uncheck(VoteRequest::parseFrom),
        handler,
        VoteResponse::toByteArray);
  }

  @Override
  public void unregisterVoteHandler() {
    clusterCommunicator.unsubscribe(context.voteSubject);
  }

  @Override
  public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    clusterCommunicator.subscribe(
        context.appendSubject,
        uncheck(AppendRequest::parseFrom),
        handler,
        AppendResponse::toByteArray);
  }

  @Override
  public void unregisterAppendHandler() {
    clusterCommunicator.unsubscribe(context.appendSubject);
  }

  @Override
  public void registerResetListener(SessionId sessionId, Consumer<ResetRequest> listener, Executor executor) {
    clusterCommunicator.subscribe(
        context.resetSubject(sessionId.id()),
        uncheck(ResetRequest::parseFrom),
        listener,
        executor);
  }

  @Override
  public void unregisterResetListener(SessionId sessionId) {
    clusterCommunicator.unsubscribe(context.resetSubject(sessionId.id()));
  }
}
