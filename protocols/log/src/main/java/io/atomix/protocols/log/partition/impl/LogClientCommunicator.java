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
package io.atomix.protocols.log.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessagingException;
import io.atomix.log.protocol.AppendRequest;
import io.atomix.log.protocol.AppendResponse;
import io.atomix.log.protocol.ConsumeRequest;
import io.atomix.log.protocol.ConsumeResponse;
import io.atomix.log.protocol.LogClientProtocol;
import io.atomix.log.protocol.RecordsRequest;
import io.atomix.log.protocol.ResetRequest;
import io.atomix.primitive.PrimitiveException;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class LogClientCommunicator implements LogClientProtocol {
  private final LogMessageContext context;
  private final ClusterCommunicationService clusterCommunicator;

  public LogClientCommunicator(String prefix, ClusterCommunicationService clusterCommunicator) {
    this.context = new LogMessageContext(prefix);
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T> void unicast(String subject, T request, Function<T, byte[]> encoder, MemberId memberId) {
    clusterCommunicator.unicast(subject, request, encoder, memberId, false);
  }

  private <T, U> CompletableFuture<U> send(String subject, T request, Function<T, byte[]> encoder, Function<byte[], U> decoder, MemberId memberId) {
    CompletableFuture<U> future = new CompletableFuture<>();
    clusterCommunicator.send(subject, request, encoder, decoder, memberId).whenComplete((result, error) -> {
      if (error == null) {
        future.complete(result);
      } else {
        Throwable cause = Throwables.getRootCause(error);
        if (cause instanceof MessagingException.NoRemoteHandler) {
          future.completeExceptionally(new PrimitiveException.Unavailable());
        } else {
          future.completeExceptionally(error);
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<AppendResponse> append(String memberId, AppendRequest request) {
    return send(context.appendSubject, request, this::encode, bytes -> decode(bytes, AppendResponse::parseFrom), MemberId.from(memberId));
  }

  @Override
  public CompletableFuture<ConsumeResponse> consume(String memberId, ConsumeRequest request) {
    return send(context.consumeSubject, request, this::encode, bytes -> decode(bytes, ConsumeResponse::parseFrom), MemberId.from(memberId));
  }

  @Override
  public void reset(String memberId, ResetRequest request) {
    unicast(context.resetSubject, request, this::encode, MemberId.from(memberId));
  }

  @Override
  public void registerRecordsConsumer(long consumerId, Consumer<RecordsRequest> handler, Executor executor) {
    clusterCommunicator.subscribe(context.recordsSubject(consumerId), bytes -> decode(bytes, RecordsRequest::parseFrom), handler, executor);
  }

  @Override
  public void unregisterRecordsConsumer(long consumerId) {
    clusterCommunicator.unsubscribe(context.recordsSubject(consumerId));
  }

  private interface Parser<T> {
    T parse(byte[] bytes) throws InvalidProtocolBufferException;
  }

  private byte[] encode(Message message) {
    return message.toByteArray();
  }

  private <T extends Message> T decode(byte[] bytes, Parser<T> parser) {
    try {
      return parser.parse(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
