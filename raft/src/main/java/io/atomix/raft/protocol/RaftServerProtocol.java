/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.raft.protocol;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.atomix.utils.stream.StreamHandler;

/**
 * Raft server protocol.
 */
public interface RaftServerProtocol {

  /**
   * Sends a query request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<QueryResponse> query(String server, QueryRequest request);

  /**
   * Sends a query request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler);

  /**
   * Sends a command request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<CommandResponse> command(String server, CommandRequest request);

  /**
   * Sends a command request to the given node.
   *
   * @param member  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<Void> commandStream(String member, CommandRequest request, StreamHandler<CommandResponse> handler);

  /**
   * Sends a join request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<JoinResponse> join(String server, JoinRequest request);

  /**
   * Sends a leave request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<LeaveResponse> leave(String server, LeaveRequest request);

  /**
   * Sends a configure request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<ConfigureResponse> configure(String server, ConfigureRequest request);

  /**
   * Sends a reconfigure request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<ReconfigureResponse> reconfigure(String server, ReconfigureRequest request);

  /**
   * Sends an install request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<InstallResponse> install(String server, InstallRequest request);

  /**
   * Sends a transfer request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<TransferResponse> transfer(String server, TransferRequest request);

  /**
   * Sends a poll request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<PollResponse> poll(String server, PollRequest request);

  /**
   * Sends a vote request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<VoteResponse> vote(String server, VoteRequest request);

  /**
   * Sends an append request to the given node.
   *
   * @param server  the node to which to send the request
   * @param request the request to send
   * @return a future to be completed with the response
   */
  CompletableFuture<AppendResponse> append(String server, AppendRequest request);

  /**
   * Registers a query request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler);

  /**
   * Unregisters the query request handler.
   */
  void unregisterQueryHandler();

  /**
   * Registers a query stream callback.
   *
   * @param handler the query stream handler to register
   */
  void registerQueryStreamHandler(BiFunction<QueryRequest, StreamHandler<QueryResponse>, CompletableFuture<Void>> handler);

  /**
   * Unregisters a query stream callback.
   */
  void unregisterQueryStreamHandler();

  /**
   * Registers a command request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler);

  /**
   * Unregisters the command request handler.
   */
  void unregisterCommandHandler();

  /**
   * Registers a command stream callback.
   *
   * @param handler the command stream handler to register
   */
  void registerCommandStreamHandler(BiFunction<CommandRequest, StreamHandler<CommandResponse>, CompletableFuture<Void>> handler);

  /**
   * Unregisters a command stream callback.
   */
  void unregisterCommandStreamHandler();

  /**
   * Registers a join request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler);

  /**
   * Unregisters the join request handler.
   */
  void unregisterJoinHandler();

  /**
   * Registers a leave request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler);

  /**
   * Unregisters the leave request handler.
   */
  void unregisterLeaveHandler();

  /**
   * Registers a transfer request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler);

  /**
   * Unregisters the transfer request handler.
   */
  void unregisterTransferHandler();

  /**
   * Registers a configure request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler);

  /**
   * Unregisters the configure request handler.
   */
  void unregisterConfigureHandler();

  /**
   * Registers a reconfigure request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler);

  /**
   * Unregisters the reconfigure request handler.
   */
  void unregisterReconfigureHandler();

  /**
   * Registers a install request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler);

  /**
   * Unregisters the install request handler.
   */
  void unregisterInstallHandler();

  /**
   * Registers a poll request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler);

  /**
   * Unregisters the poll request handler.
   */
  void unregisterPollHandler();

  /**
   * Registers a vote request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler);

  /**
   * Unregisters the vote request handler.
   */
  void unregisterVoteHandler();

  /**
   * Registers an append request callback.
   *
   * @param handler the open session request handler to register
   */
  void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler);

  /**
   * Unregisters the append request handler.
   */
  void unregisterAppendHandler();

}