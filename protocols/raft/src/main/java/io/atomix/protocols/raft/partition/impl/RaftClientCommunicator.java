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

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.raft.protocol.CommandRequest;
import io.atomix.raft.protocol.CommandResponse;
import io.atomix.raft.protocol.QueryRequest;
import io.atomix.raft.protocol.QueryResponse;
import io.atomix.raft.protocol.RaftClientProtocol;
import io.atomix.utils.stream.StreamHandler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class RaftClientCommunicator implements RaftClientProtocol {
  private final RaftMessageContext context;
  private final ClusterCommunicationService clusterCommunicator;
  private final ClusterStreamingService streamingService;

  public RaftClientCommunicator(ClusterCommunicationService clusterCommunicator, ClusterStreamingService streamingService) {
    this(null, clusterCommunicator, streamingService);
  }

  public RaftClientCommunicator(String prefix, ClusterCommunicationService clusterCommunicator, ClusterStreamingService streamingService) {
    this.context = new RaftMessageContext(prefix);
    this.clusterCommunicator = checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    this.streamingService = checkNotNull(streamingService, "streamingService cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(
      String subject, T request, Function<T, byte[]> encoder, Function<byte[], U> decoder, MemberId memberId) {
    return clusterCommunicator.send(subject, request, encoder, decoder, memberId);
  }

  @Override
  public CompletableFuture<QueryResponse> query(String server, QueryRequest request) {
    return sendAndReceive(
        context.querySubject,
        request,
        QueryRequest::toByteArray,
        bytes -> ByteArrayDecoder.decode(bytes, QueryResponse::parseFrom),
        MemberId.from(server));
  }

  @Override
  public CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler) {
    return streamingService.send(
        context.queryStreamSubject,
        request,
        QueryRequest::toByteArray,
        bytes -> ByteArrayDecoder.decode(bytes, QueryResponse::parseFrom),
        handler,
        MemberId.from(server));
  }

  @Override
  public CompletableFuture<CommandResponse> command(String server, CommandRequest request) {
    return sendAndReceive(
        context.commandSubject,
        request,
        CommandRequest::toByteArray,
        bytes -> ByteArrayDecoder.decode(bytes, CommandResponse::parseFrom),
        MemberId.from(server));

  }

  @Override
  public CompletableFuture<Void> commandStream(String server, CommandRequest request, StreamHandler<CommandResponse> handler) {
    return streamingService.send(
        context.commandStreamSubject,
        request,
        CommandRequest::toByteArray,
        bytes -> ByteArrayDecoder.decode(bytes, CommandResponse::parseFrom),
        handler,
        MemberId.from(server));
  }
}
