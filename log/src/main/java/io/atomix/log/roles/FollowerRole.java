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
package io.atomix.log.roles;

import java.util.concurrent.CompletableFuture;

import io.atomix.log.DistributedLogServer;
import io.atomix.log.impl.DistributedLogServerContext;
import io.atomix.log.protocol.BackupOperation;
import io.atomix.log.protocol.BackupRequest;
import io.atomix.log.protocol.BackupResponse;
import io.atomix.log.protocol.ResponseStatus;
import io.atomix.log.protocol.LogEntry;
import io.atomix.storage.StorageException;
import io.atomix.storage.journal.JournalReader;
import io.atomix.storage.journal.JournalWriter;

/**
 * Backup role.
 */
public class FollowerRole extends LogServerRole {
  public FollowerRole(DistributedLogServerContext service) {
    super(DistributedLogServer.Role.FOLLOWER, service);
  }

  @Override
  public CompletableFuture<BackupResponse> backup(BackupRequest request) {
    logRequest(request);

    // If the term is greater than the node's current term, update the term.
    if (request.getTerm() > context.currentTerm()) {
      context.resetTerm(request.getTerm(), request.getLeader());
    }
    // If the term is less than the node's current term, ignore the backup message.
    else if (request.getTerm() < context.currentTerm()) {
      return CompletableFuture.completedFuture(BackupResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .build());
    }

    JournalWriter<LogEntry> writer = context.writer();
    JournalReader<LogEntry> reader = context.reader();

    // Iterate through all operations in the batch and append entries.
    for (BackupOperation operation : request.getOperationsList()) {

      // If the reader's next index does not align with the operation index, reset the reader.
      if (reader.getNextIndex() != operation.getIndex()) {
        reader.reset(operation.getIndex());
      }

      // If the reader has no next entry, append the entry to the journal.
      if (!reader.hasNext()) {
        try {
          writer.append(LogEntry.newBuilder()
              .setTerm(operation.getTerm())
              .setTimestamp(operation.getTimestamp())
              .setValue(operation.getValue())
              .build());
        } catch (StorageException e) {
          return CompletableFuture.completedFuture(BackupResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .build());
        }
      }
      // If the next entry's term does not match the operation term, append the entry to the journal.
      else if (reader.next().entry().getTerm() != operation.getTerm()) {
        writer.truncate(operation.getIndex());
        try {
          writer.append(LogEntry.newBuilder()
              .setTerm(operation.getTerm())
              .setTimestamp(operation.getTimestamp())
              .setValue(operation.getValue())
              .build());
        } catch (StorageException e) {
          return CompletableFuture.completedFuture(BackupResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .build());
        }
      }
    }
    return CompletableFuture.completedFuture(logResponse(BackupResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .build()));
  }
}
