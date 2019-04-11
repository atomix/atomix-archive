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
package io.atomix.protocols.backup.roles;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.Close;
import io.atomix.protocols.backup.protocol.Execute;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.Expire;
import io.atomix.protocols.backup.protocol.Heartbeat;
import io.atomix.protocols.backup.protocol.ResponseStatus;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.protocol.RestoreResponse;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.protocols.backup.snapshot.ServiceSession;
import io.atomix.protocols.backup.snapshot.ServiceSnapshot;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;

/**
 * Primary role.
 */
public class PrimaryRole extends PrimaryBackupRole {
  private static final long HEARTBEAT_FREQUENCY = 1000;

  private final Replicator replicator;
  private Scheduled heartbeatTimer;

  public PrimaryRole(PrimaryBackupServiceContext context) {
    super(Role.PRIMARY, context);
    heartbeatTimer = context.threadContext().schedule(
        Duration.ofMillis(HEARTBEAT_FREQUENCY),
        Duration.ofMillis(HEARTBEAT_FREQUENCY),
        this::heartbeat);
    switch (context.descriptor().getReplication()) {
      case SYNCHRONOUS:
        replicator = new SynchronousReplicator(context, log);
        break;
      case ASYNCHRONOUS:
        replicator = new AsynchronousReplicator(context, log);
        break;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Applies a heartbeat to the service to ensure timers can be triggered.
   */
  private void heartbeat() {
    long index = context.nextIndex();
    long timestamp = System.currentTimeMillis();
    replicator.replicate(BackupOperation.newBuilder()
        .setIndex(index)
        .setTimestamp(timestamp)
        .setHeartbeat(Heartbeat.newBuilder().build())
        .build())
        .thenRun(() -> context.setTimestamp(timestamp));
  }

  @Override
  public CompletableFuture<ExecuteResponse> execute(ExecuteRequest request) {
    logRequest(request);
    if (request.getType() == ExecuteRequest.Type.COMMAND) {
      return executeCommand(request).thenApply(this::logResponse);
    } else if (request.getType() == ExecuteRequest.Type.QUERY) {
      return executeQuery(request).thenApply(this::logResponse);
    }
    return Futures.exceptionalFuture(new IllegalArgumentException("Unknown operation type"));
  }

  private CompletableFuture<ExecuteResponse> executeCommand(ExecuteRequest request) {
    PrimaryBackupSession session = context.getOrCreateSession(request.getSessionId(), MemberId.from(request.getMemberId()));
    long index = context.nextIndex();
    long timestamp = System.currentTimeMillis();
    return replicator.replicate(BackupOperation.newBuilder()
        .setIndex(index)
        .setTimestamp(timestamp)
        .setExecute(Execute.newBuilder()
            .setSessionId(session.sessionId().id())
            .setMemberId(session.memberId().id())
            .setOperation(request.getOperation())
            .setValue(request.getValue())
            .build())
        .build())
        .thenApply(v -> {
          try {
            byte[] result = context.service().apply(new DefaultCommit<>(
                context.setIndex(index),
                OperationId.newBuilder()
                    .setType(OperationType.COMMAND)
                    .setName(request.getOperation())
                    .build(),
                request.getValue().toByteArray(),
                context.setSession(session),
                context.setTimestamp(timestamp)));
            return ExecuteResponse.newBuilder()
                .setStatus(ResponseStatus.OK)
                .setResult(ByteString.copyFrom(result))
                .build();
          } catch (Exception e) {
            return ExecuteResponse.newBuilder()
                .setStatus(ResponseStatus.ERROR)
                .build();
          } finally {
            context.setSession(null);
          }
        });
  }

  private CompletableFuture<ExecuteResponse> executeQuery(ExecuteRequest request) {
    // If the session doesn't exist, create and replicate a new session before applying the query.
    Session session = context.getSession(request.getSessionId());
    if (session == null) {
      Session newSession = context.createSession(request.getSessionId(), MemberId.from(request.getMemberId()));
      long index = context.nextIndex();
      long timestamp = System.currentTimeMillis();
      return replicator.replicate(BackupOperation.newBuilder()
          .setIndex(index)
          .setTimestamp(timestamp)
          .setExecute(Execute.newBuilder()
              .setSessionId(newSession.sessionId().id())
              .setMemberId(newSession.memberId().id())
              .build())
          .build())
          .thenApply(v -> {
            context.setIndex(index);
            context.setTimestamp(timestamp);
            return applyQuery(request, newSession);
          });
    } else {
      return CompletableFuture.completedFuture(applyQuery(request, session));
    }
  }

  private ExecuteResponse applyQuery(ExecuteRequest request, Session session) {
    try {
      byte[] result = context.service().apply(new DefaultCommit<>(
          context.getIndex(),
          OperationId.newBuilder()
              .setType(OperationType.QUERY)
              .setName(request.getOperation())
              .build(),
          request.getValue().toByteArray(),
          context.setSession(session),
          context.currentTimestamp()));
      return ExecuteResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .setResult(ByteString.copyFrom(result))
          .build();
    } catch (Exception e) {
      return ExecuteResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .build();
    } finally {
      context.setSession(null);
    }
  }

  @Override
  public CompletableFuture<RestoreResponse> restore(RestoreRequest request) {
    logRequest(request);
    if (request.getTerm() != context.currentTerm()) {
      return CompletableFuture.completedFuture(logResponse(RestoreResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .build()));
    }

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      context.service().backup(output);
    } catch (IOException e) {
      log.error("Failed to snapshot service {}", context.serviceName(), e);
    }

    ServiceSnapshot snapshot = ServiceSnapshot.newBuilder()
        .addAllSessions(context.getSessions().stream()
            .map(session -> ServiceSession.newBuilder()
                .setSessionId(session.sessionId().id())
                .setMemberId(session.memberId().id())
                .build())
            .collect(Collectors.toList()))
        .setSnapshot(ByteString.copyFrom(output.toByteArray()))
        .build();

    output = new ByteArrayOutputStream();
    try {
      snapshot.writeTo(output);
    } catch (IOException e) {
      log.error("Failed to serialize snapshot", e);
    }

    return CompletableFuture.completedFuture(RestoreResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setIndex(context.currentIndex())
        .setTimestamp(context.currentTimestamp())
        .setData(ByteString.copyFrom(output.toByteArray()))
        .build())
        .thenApply(this::logResponse);
  }

  @Override
  public CompletableFuture<Void> expire(PrimaryBackupSession session) {
    long index = context.nextIndex();
    long timestamp = System.currentTimeMillis();
    return replicator.replicate(BackupOperation.newBuilder()
        .setIndex(index)
        .setTimestamp(timestamp)
        .setExpire(Expire.newBuilder()
            .setSessionId(session.sessionId().id())
            .build())
        .build())
        .thenRun(() -> {
          context.setTimestamp(timestamp);
          context.expireSession(session.sessionId().id());
        });
  }

  @Override
  public CompletableFuture<Void> close(PrimaryBackupSession session) {
    long index = context.nextIndex();
    long timestamp = System.currentTimeMillis();
    return replicator.replicate(BackupOperation.newBuilder()
        .setIndex(index)
        .setTimestamp(timestamp)
        .setClose(Close.newBuilder()
            .setSessionId(session.sessionId().id())
            .build())
        .build())
        .thenRun(() -> {
          context.setTimestamp(timestamp);
          context.closeSession(session.sessionId().id());
        });
  }

  @Override
  public void close() {
    replicator.close();
    heartbeatTimer.cancel();
  }
}
