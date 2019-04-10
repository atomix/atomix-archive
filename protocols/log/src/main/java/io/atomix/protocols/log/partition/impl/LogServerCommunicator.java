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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.AppendResponse;
import io.atomix.protocols.log.protocol.BackupRequest;
import io.atomix.protocols.log.protocol.BackupResponse;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.ConsumeResponse;
import io.atomix.protocols.log.protocol.LogServerProtocol;
import io.atomix.protocols.log.protocol.RecordsRequest;
import io.atomix.protocols.log.protocol.ResetRequest;

/**
 * Raft server protocol that uses a {@link ClusterCommunicationService}.
 */
public class LogServerCommunicator implements LogServerProtocol {
  private final LogMessageContext context;
  private final ClusterCommunicationService clusterCommunicator;

  public LogServerCommunicator(String prefix, ClusterCommunicationService clusterCommunicator) {
    this.context = new LogMessageContext(prefix);
    this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
  }

  @Override
  public void produce(MemberId memberId, String subject, RecordsRequest request) {
    clusterCommunicator.unicast(subject, request, this::encode, memberId);
  }

  @Override
  public CompletableFuture<BackupResponse> backup(MemberId memberId, BackupRequest request) {
    return clusterCommunicator.send(
        context.backupSubject,
        request,
        this::encode,
        bytes -> decode(bytes, BackupResponse::parseFrom),
        memberId);
  }

  @Override
  public void registerBackupHandler(Function<BackupRequest, CompletableFuture<BackupResponse>> handler) {
    clusterCommunicator.subscribe(
        context.backupSubject,
        bytes -> decode(bytes, BackupRequest::parseFrom),
        handler,
        this::encode);
  }

  @Override
  public void unregisterBackupHandler() {
    clusterCommunicator.unsubscribe(context.backupSubject);
  }

  @Override
  public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
    clusterCommunicator.subscribe(
        context.appendSubject,
        bytes -> decode(bytes, AppendRequest::parseFrom),
        handler,
        this::encode);
  }

  @Override
  public void unregisterAppendHandler() {
    clusterCommunicator.unsubscribe(context.appendSubject);
  }

  @Override
  public void registerConsumeHandler(Function<ConsumeRequest, CompletableFuture<ConsumeResponse>> handler) {
    clusterCommunicator.subscribe(
        context.consumeSubject,
        bytes -> decode(bytes, ConsumeRequest::parseFrom),
        handler,
        this::encode);
  }

  @Override
  public void unregisterConsumeHandler() {
    clusterCommunicator.unsubscribe(context.consumeSubject);
  }

  @Override
  public void registerResetConsumer(Consumer<ResetRequest> consumer, Executor executor) {
    clusterCommunicator.subscribe(
        context.resetSubject,
        bytes -> decode(bytes, ResetRequest::parseFrom),
        consumer,
        executor);
  }

  @Override
  public void unregisterResetConsumer() {
    clusterCommunicator.unsubscribe(context.resetSubject);
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
