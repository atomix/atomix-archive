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
package io.atomix.protocols.backup.partition.impl;

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
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.protocol.BackupEvent;
import io.atomix.protocols.backup.protocol.CloseRequest;
import io.atomix.protocols.backup.protocol.CloseResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.MetadataRequest;
import io.atomix.protocols.backup.protocol.MetadataResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupClientProtocol;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class PrimaryBackupClientCommunicator implements PrimaryBackupClientProtocol {
  private final PrimaryBackupMessageContext context;
  private final ClusterCommunicationService clusterCommunicator;

  public PrimaryBackupClientCommunicator(String prefix, ClusterCommunicationService clusterCommunicator) {
    this.context = new PrimaryBackupMessageContext(prefix);
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  private <T, U> CompletableFuture<U> sendAndReceive(
      String subject, T request, Function<T, byte[]> encoder, Function<byte[], U> decoder, MemberId memberId) {
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
  public CompletableFuture<ExecuteResponse> execute(MemberId memberId, ExecuteRequest request) {
    return sendAndReceive(
        context.executeSubject,
        request,
        this::encode,
        bytes -> decode(bytes, ExecuteResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
    return sendAndReceive(
        context.metadataSubject,
        request,
        this::encode,
        bytes -> decode(bytes, MetadataResponse::parseFrom),
        memberId);
  }

  @Override
  public CompletableFuture<CloseResponse> close(MemberId memberId, CloseRequest request) {
    return sendAndReceive(
        context.closeSubject,
        request,
        this::encode,
        bytes -> decode(bytes, CloseResponse::parseFrom),
        memberId);
  }

  @Override
  public void registerEventListener(SessionId sessionId, Consumer<PrimitiveEvent> listener, Executor executor) {
    clusterCommunicator.subscribe(
        context.eventSubject(sessionId.id()),
        bytes -> decode(bytes, BackupEvent::parseFrom),
        event -> listener.accept(new PrimitiveEvent(EventType.from(event.getType()), event.getValue().toByteArray())),
        executor);
  }

  @Override
  public void unregisterEventListener(SessionId sessionId) {
    clusterCommunicator.unsubscribe(context.eventSubject(sessionId.id()));
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
