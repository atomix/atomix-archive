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
import java.util.function.Function;

import com.google.common.base.Preconditions;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.raft.protocol.CommandRequest;
import io.atomix.raft.protocol.CommandResponse;
import io.atomix.raft.protocol.QueryRequest;
import io.atomix.raft.protocol.QueryResponse;
import io.atomix.raft.protocol.RaftClientProtocol;

import static io.atomix.utils.concurrent.Futures.uncheck;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class RaftClientCommunicator implements RaftClientProtocol {
  private final RaftMessageContext context;
  private final ClusterCommunicationService clusterCommunicator;

  public RaftClientCommunicator(ClusterCommunicationService clusterCommunicator) {
    this(null, clusterCommunicator);
  }

  public RaftClientCommunicator(String prefix, ClusterCommunicationService clusterCommunicator) {
    this.context = new RaftMessageContext(prefix);
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(
      String subject, T request, Function<T, byte[]> encoder, Function<byte[], U> decoder, MemberId memberId) {
    return clusterCommunicator.send(subject, request, encoder, decoder, memberId);
  }

  @Override
  public CompletableFuture<QueryResponse> query(String memberId, QueryRequest request) {
    return sendAndReceive(
        context.querySubject,
        request,
        QueryRequest::toByteArray,
        uncheck(QueryResponse::parseFrom),
        MemberId.from(memberId));
  }

  @Override
  public CompletableFuture<CommandResponse> command(String memberId, CommandRequest request) {
    return sendAndReceive(
        context.commandSubject,
        request,
        CommandRequest::toByteArray,
        uncheck(CommandResponse::parseFrom),
        MemberId.from(memberId));
  }
}
