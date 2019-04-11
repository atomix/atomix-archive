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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.session.Session;
import io.atomix.protocols.backup.PrimaryBackupServer.Role;
import io.atomix.protocols.backup.impl.PrimaryBackupSession;
import io.atomix.protocols.backup.protocol.BackupOperation;
import io.atomix.protocols.backup.protocol.BackupRequest;
import io.atomix.protocols.backup.protocol.BackupResponse;
import io.atomix.protocols.backup.protocol.ResponseStatus;
import io.atomix.protocols.backup.protocol.RestoreRequest;
import io.atomix.protocols.backup.service.impl.PrimaryBackupServiceContext;
import io.atomix.protocols.backup.snapshot.ServiceSession;
import io.atomix.protocols.backup.snapshot.ServiceSnapshot;

/**
 * Backup role.
 */
public class BackupRole extends PrimaryBackupRole {
  private final Queue<BackupOperation> operations = new LinkedList<>();

  public BackupRole(PrimaryBackupServiceContext service) {
    super(Role.BACKUP, service);
  }

  @Override
  public CompletableFuture<BackupResponse> backup(BackupRequest request) {
    logRequest(request);

    // If the term is greater than the node's current term, update the term.
    if (request.getTerm() > context.currentTerm()) {
      context.resetTerm(request.getTerm(), MemberId.from(request.getPrimary()));
    }
    // If the term is less than the node's current term, ignore the backup message.
    else if (request.getTerm() < context.currentTerm()) {
      return CompletableFuture.completedFuture(BackupResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .build());
    }

    operations.addAll(request.getOperationsList());
    long currentCommitIndex = context.getCommitIndex();
    long nextCommitIndex = context.setCommitIndex(request.getIndex());
    context.threadContext().execute(() -> applyOperations(currentCommitIndex, nextCommitIndex));
    return CompletableFuture.completedFuture(logResponse(BackupResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .build()));
  }

  /**
   * Applies operations in the given range.
   */
  private void applyOperations(long fromIndex, long toIndex) {
    for (long i = fromIndex + 1; i <= toIndex; i++) {
      BackupOperation operation = operations.poll();
      if (operation == null) {
        requestRestore(context.primary());
        break;
      }

      if (context.nextIndex(operation.getIndex())) {
        if (operation.hasExecute()) {
          applyExecute(operation);
        } else if (operation.hasHeartbeat()) {
          applyHeartbeat(operation);
        } else if (operation.hasExpire()) {
          applyExpire(operation);
        } else if (operation.hasClose()) {
          applyClose(operation);
        }
      } else if (operation.getIndex() < i) {
        continue;
      } else {
        requestRestore(context.primary());
        break;
      }
    }
  }

  /**
   * Applies an execute operation to the service.
   */
  private void applyExecute(BackupOperation operation) {
    Session session = context.getOrCreateSession(operation.getExecute().getSessionId(), MemberId.from(operation.getExecute().getMemberId()));
    if (!operation.getExecute().getOperation().equals("")) {
      try {
        context.service().apply(new DefaultCommit<>(
            context.setIndex(operation.getIndex()),
            OperationId.newBuilder()
                .setType(OperationType.COMMAND)
                .setName(operation.getExecute().getOperation())
                .build(),
            !operation.getExecute().getValue().isEmpty() ? operation.getExecute().getValue().toByteArray() : null,
            context.setSession(session),
            context.setTimestamp(operation.getTimestamp())));
      } catch (Exception e) {
        log.warn("Failed to apply operation: {}", e);
      } finally {
        context.setSession(null);
      }
    }
  }

  /**
   * Applies a heartbeat operation to the service.
   */
  private void applyHeartbeat(BackupOperation operation) {
    context.setTimestamp(operation.getTimestamp());
  }

  /**
   * Applies an expire operation.
   */
  private void applyExpire(BackupOperation operation) {
    context.setTimestamp(operation.getTimestamp());
    PrimaryBackupSession session = context.getSession(operation.getExpire().getSessionId());
    if (session != null) {
      context.expireSession(session.sessionId().id());
    }
  }

  /**
   * Applies a close operation.
   */
  private void applyClose(BackupOperation operation) {
    context.setTimestamp(operation.getTimestamp());
    PrimaryBackupSession session = context.getSession(operation.getClose().getSessionId());
    if (session != null) {
      context.closeSession(session.sessionId().id());
    }
  }

  /**
   * Requests a restore from the primary.
   */
  private void requestRestore(MemberId primary) {
    context.protocol().restore(primary, RestoreRequest.newBuilder()
        .setPrimitive(context.descriptor())
        .setTerm(context.currentTerm())
        .build())
        .whenCompleteAsync((response, error) -> {
          if (error == null && response.getStatus() == ResponseStatus.OK) {
            context.resetIndex(response.getIndex(), response.getTimestamp());

            try {
              ServiceSnapshot snapshot = ServiceSnapshot.parseFrom(response.getData());
              for (ServiceSession session : snapshot.getSessionsList()) {
                context.getOrCreateSession(session.getSessionId(), MemberId.from(session.getMemberId()));
              }

              context.service().restore(new ByteArrayInputStream(snapshot.getSnapshot().toByteArray()));
              operations.clear();
            } catch (IOException e) {
              log.error("Failed to deserialize snapshot");
            }
          }
        }, context.threadContext());
  }
}
