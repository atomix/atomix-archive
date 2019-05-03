/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.raft.roles;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.google.protobuf.ByteString;
import io.atomix.raft.RaftException;
import io.atomix.raft.RaftServer;
import io.atomix.raft.cluster.impl.DefaultRaftMember;
import io.atomix.raft.impl.OperationResult;
import io.atomix.raft.impl.RaftContext;
import io.atomix.raft.protocol.AppendRequest;
import io.atomix.raft.protocol.AppendResponse;
import io.atomix.raft.protocol.CommandRequest;
import io.atomix.raft.protocol.CommandResponse;
import io.atomix.raft.protocol.InstallRequest;
import io.atomix.raft.protocol.InstallResponse;
import io.atomix.raft.protocol.JoinRequest;
import io.atomix.raft.protocol.JoinResponse;
import io.atomix.raft.protocol.LeaveRequest;
import io.atomix.raft.protocol.LeaveResponse;
import io.atomix.raft.protocol.PollRequest;
import io.atomix.raft.protocol.PollResponse;
import io.atomix.raft.protocol.QueryRequest;
import io.atomix.raft.protocol.QueryResponse;
import io.atomix.raft.protocol.RaftError;
import io.atomix.raft.protocol.ReadConsistency;
import io.atomix.raft.protocol.ReconfigureRequest;
import io.atomix.raft.protocol.ReconfigureResponse;
import io.atomix.raft.protocol.ResponseStatus;
import io.atomix.raft.protocol.VoteRequest;
import io.atomix.raft.protocol.VoteResponse;
import io.atomix.raft.storage.log.QueryEntry;
import io.atomix.raft.storage.log.RaftLogEntry;
import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.log.RaftLogWriter;
import io.atomix.raft.storage.snapshot.Snapshot;
import io.atomix.storage.StorageException;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.time.WallClockTimestamp;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Passive state.
 */
public class PassiveRole extends InactiveRole {
  private PendingSnapshot pendingSnapshot;

  public PassiveRole(RaftContext context) {
    super(context);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.PASSIVE;
  }

  @Override
  public CompletableFuture<RaftRole> start() {
    return super.start()
        .thenRun(this::truncateUncommittedEntries)
        .thenApply(v -> this);
  }

  /**
   * Truncates uncommitted entries from the log.
   */
  private void truncateUncommittedEntries() {
    if (role() == RaftServer.Role.PASSIVE) {
      final RaftLogWriter writer = raft.getLogWriter();
      writer.truncate(raft.getCommitIndex());
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.getTerm(), request.getLeader());
    return handleAppend(request);
  }

  /**
   * Handles an AppendRequest.
   */
  protected CompletableFuture<AppendResponse> handleAppend(final AppendRequest request) {
    CompletableFuture<AppendResponse> future = new CompletableFuture<>();

    // Check that the term of the given request matches the local term or update the term.
    if (!checkTerm(request, future)) {
      return future;
    }

    // Check that the previous index/term matches the local log's last entry.
    if (!checkPreviousEntry(request, future)) {
      return future;
    }

    // Append the entries to the log.
    appendEntries(request, future);
    return future;
  }

  /**
   * Checks the leader's term of the given AppendRequest, returning a boolean indicating whether to continue
   * handling the request.
   */
  protected boolean checkTerm(AppendRequest request, CompletableFuture<AppendResponse> future) {
    RaftLogWriter writer = raft.getLogWriter();
    if (request.getTerm() < raft.getTerm()) {
      log.debug("Rejected {}: request term is less than the current term ({})", request, raft.getTerm());
      return failAppend(writer.getLastIndex(), future);
    }
    return true;
  }

  /**
   * Checks the previous index of the given AppendRequest, returning a boolean indicating whether to continue
   * handling the request.
   */
  protected boolean checkPreviousEntry(AppendRequest request, CompletableFuture<AppendResponse> future) {
    RaftLogWriter writer = raft.getLogWriter();
    RaftLogReader reader = raft.getLogReader();

    // If the previous term is set, validate that it matches the local log.
    // We check the previous log term since that indicates whether any entry is present in the leader's
    // log at the previous log index. It's possible that the leader can send a non-zero previous log index
    // with a zero term in the event the leader has compacted its logs and is sending the first entry.
    if (request.getPrevLogTerm() != 0) {
      // Get the last entry written to the log.
      Indexed<RaftLogEntry> lastEntry = writer.getLastEntry();

      // If the local log is non-empty...
      if (lastEntry != null) {
        // If the previous log index is greater than the last entry index, fail the attempt.
        if (request.getPrevLogIndex() > lastEntry.index()) {
          log.debug("Rejected {}: Previous index ({}) is greater than the local log's last index ({})", request, request.getPrevLogIndex(), lastEntry.index());
          return failAppend(lastEntry.index(), future);
        }

        // If the previous log index is less than the last written entry index, look up the entry.
        if (request.getPrevLogIndex() < lastEntry.index()) {
          // Reset the reader to the previous log index.
          if (reader.getNextIndex() != request.getPrevLogIndex()) {
            reader.reset(request.getPrevLogIndex());
          }

          // The previous entry should exist in the log if we've gotten this far.
          if (!reader.hasNext()) {
            log.debug("Rejected {}: Previous entry does not exist in the local log", request);
            return failAppend(lastEntry.index(), future);
          }

          // Read the previous entry and validate that the term matches the request previous log term.
          Indexed<RaftLogEntry> previousEntry = reader.next();
          if (request.getPrevLogTerm() != previousEntry.entry().getTerm()) {
            log.debug("Rejected {}: Previous entry term ({}) does not match local log's term for the same entry ({})", request, request.getPrevLogTerm(), previousEntry.entry().getTerm());
            return failAppend(request.getPrevLogIndex() - 1, future);
          }
        }
        // If the previous log term doesn't equal the last entry term, fail the append, sending the prior entry.
        else if (request.getPrevLogTerm() != lastEntry.entry().getTerm()) {
          log.debug("Rejected {}: Previous entry term ({}) does not equal the local log's last term ({})", request, request.getPrevLogTerm(), lastEntry.entry().getTerm());
          return failAppend(request.getPrevLogIndex() - 1, future);
        }
      } else {
        // If the previous log index is set and the last entry is null, fail the append.
        if (request.getPrevLogIndex() > 0) {
          log.debug("Rejected {}: Previous index ({}) is greater than the local log's last index (0)", request, request.getPrevLogIndex());
          return failAppend(0, future);
        }
      }
    }
    return true;
  }

  /**
   * Appends entries from the given AppendRequest.
   */
  protected void appendEntries(AppendRequest request, CompletableFuture<AppendResponse> future) {
    // Compute the last entry index from the previous log index and request entry count.
    final long lastEntryIndex = request.getPrevLogIndex() + request.getEntriesCount();

    // Ensure the commitIndex is not increased beyond the index of the last entry in the request.
    final long commitIndex = Math.max(raft.getCommitIndex(), Math.min(request.getCommitIndex(), lastEntryIndex));

    // Track the last log index while entries are appended.
    long lastLogIndex = request.getPrevLogIndex();

    if (!request.getEntriesList().isEmpty()) {
      final RaftLogWriter writer = raft.getLogWriter();
      final RaftLogReader reader = raft.getLogReader();

      // If the previous term is zero, that indicates the previous index represents the beginning of the log.
      // Reset the log to the previous index plus one.
      if (request.getPrevLogTerm() == 0) {
        log.debug("Reset first index to {}", request.getPrevLogIndex() + 1);
        writer.reset(request.getPrevLogIndex() + 1);
      }

      // Iterate through entries and append them.
      for (RaftLogEntry entry : request.getEntriesList()) {
        long index = ++lastLogIndex;

        // Get the last entry written to the log by the writer.
        Indexed<RaftLogEntry> lastEntry = writer.getLastEntry();

        if (lastEntry != null) {
          // If the last written entry index is greater than the next append entry index,
          // we need to validate that the entry that's already in the log matches this entry.
          if (lastEntry.index() > index) {
            // Reset the reader to the current entry index.
            if (reader.getNextIndex() != index) {
              reader.reset(index);
            }

            // If the reader does not have any next entry, that indicates an inconsistency between the reader and writer.
            if (!reader.hasNext()) {
              throw new IllegalStateException("Log reader inconsistent with log writer");
            }

            // Read the existing entry from the log.
            Indexed<RaftLogEntry> existingEntry = reader.next();

            // If the existing entry term doesn't match the leader's term for the same entry, truncate
            // the log and append the leader's entry.
            if (existingEntry.entry().getTerm() != entry.getTerm()) {
              writer.truncate(index - 1);
              if (!appendEntry(index, entry, writer, future)) {
                return;
              }
            }
          }
          // If the last written entry is equal to the append entry index, we don't need
          // to read the entry from disk and can just compare the last entry in the writer.
          else if (lastEntry.index() == index) {
            // If the last entry term doesn't match the leader's term for the same entry, truncate
            // the log and append the leader's entry.
            if (lastEntry.entry().getTerm() != entry.getTerm()) {
              writer.truncate(index - 1);
              if (!appendEntry(index, entry, writer, future)) {
                return;
              }
            }
          }
          // Otherwise, this entry is being appended at the end of the log.
          else {
            // If the last entry index isn't the previous index, throw an exception because something crazy happened!
            if (lastEntry.index() != index - 1) {
              throw new IllegalStateException("Log writer inconsistent with next append entry index " + index);
            }

            // Append the entry and log a message.
            if (!appendEntry(index, entry, writer, future)) {
              return;
            }
          }
        }
        // Otherwise, if the last entry is null just append the entry and log a message.
        else {
          if (!appendEntry(index, entry, writer, future)) {
            return;
          }
        }

        // If the last log index meets the commitIndex, break the append loop to avoid appending uncommitted entries.
        if (!role().active() && index == commitIndex) {
          break;
        }
      }
    }

    // Set the first commit index.
    raft.setFirstCommitIndex(request.getCommitIndex());

    // Update the context commit and global indices.
    long previousCommitIndex = raft.setCommitIndex(commitIndex);
    if (previousCommitIndex < commitIndex) {
      log.trace("Committed entries up to index {}", commitIndex);
      raft.getServiceManager().applyAll(commitIndex);
    }

    // Return a successful append response.
    succeedAppend(lastLogIndex, future);
  }

  /**
   * Attempts to append an entry, returning {@code false} if the append fails due to an {@link StorageException.OutOfDiskSpace} exception.
   */
  private boolean appendEntry(long index, RaftLogEntry entry, RaftLogWriter writer, CompletableFuture<AppendResponse> future) {
    try {
      Indexed<RaftLogEntry> indexed = writer.append(entry);
      log.trace("Appended {}", indexed);
    } catch (StorageException.TooLarge e) {
      log.warn("Entry size exceeds maximum allowed bytes. Ensure Raft storage configuration is consistent on all nodes!");
      return false;
    } catch (StorageException.OutOfDiskSpace e) {
      log.trace("Append failed: {}", e);
      raft.getServiceManager().compact();
      failAppend(index - 1, future);
      return false;
    }
    return true;
  }

  /**
   * Returns a failed append response.
   *
   * @param lastLogIndex the last log index
   * @param future       the append response future
   * @return the append response status
   */
  protected boolean failAppend(long lastLogIndex, CompletableFuture<AppendResponse> future) {
    return completeAppend(false, lastLogIndex, future);
  }

  /**
   * Returns a successful append response.
   *
   * @param lastLogIndex the last log index
   * @param future       the append response future
   * @return the append response status
   */
  protected boolean succeedAppend(long lastLogIndex, CompletableFuture<AppendResponse> future) {
    return completeAppend(true, lastLogIndex, future);
  }

  /**
   * Returns a successful append response.
   *
   * @param succeeded    whether the append succeeded
   * @param lastLogIndex the last log index
   * @param future       the append response future
   * @return the append response status
   */
  protected boolean completeAppend(boolean succeeded, long lastLogIndex, CompletableFuture<AppendResponse> future) {
    future.complete(logResponse(AppendResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setTerm(raft.getTerm())
        .setSucceeded(succeeded)
        .setLastLogIndex(lastLogIndex)
        .build()));
    return succeeded;
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
    raft.checkThread();
    logRequest(request);

    // If this server has not yet applied entries up to the client's session ID, forward the
    // query to the leader. This ensures that a follower does not tell the client its session
    // doesn't exist if the follower hasn't had a chance to see the session's registration entry.
    if (raft.getState() != RaftContext.State.READY) {
      log.trace("State out of sync, forwarding query to leader");
      return queryForward(request);
    }

    // If the session's consistency level is SEQUENTIAL, handle the request here, otherwise forward it.
    if (request.getReadConsistency() == ReadConsistency.SEQUENTIAL) {

      // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
      // Forward the request to the leader.
      if (raft.getLogWriter().getLastIndex() < raft.getCommitIndex()) {
        log.trace("State out of sync, forwarding query to leader");
        return queryForward(request);
      }

      final Indexed<RaftLogEntry> entry = new Indexed<>(
          raft.getLogWriter().getLastIndex(),
          RaftLogEntry.newBuilder()
              .setTerm(raft.getTerm())
              .setTimestamp(System.currentTimeMillis())
              .setQuery(QueryEntry.newBuilder()
                  .setValue(request.getValue())
                  .build())
              .build(), 0);

      return applyQuery(entry).thenApply(this::logResponse);
    } else {
      return queryForward(request);
    }
  }

  @Override
  public CompletableFuture<Void> onQuery(QueryRequest request, StreamHandler<QueryResponse> handler) {
    raft.checkThread();
    logRequest(request);

    // If this server has not yet applied entries up to the client's session ID, forward the
    // query to the leader. This ensures that a follower does not tell the client its session
    // doesn't exist if the follower hasn't had a chance to see the session's registration entry.
    if (raft.getState() != RaftContext.State.READY) {
      log.trace("State out of sync, forwarding query to leader");
      return queryForward(request, handler);
    }

    // If the session's consistency level is SEQUENTIAL, handle the request here, otherwise forward it.
    if (request.getReadConsistency() == ReadConsistency.SEQUENTIAL) {

      // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
      // Forward the request to the leader.
      if (raft.getLogWriter().getLastIndex() < raft.getCommitIndex()) {
        log.trace("State out of sync, forwarding query to leader");
        return queryForward(request, handler);
      }

      final Indexed<RaftLogEntry> entry = new Indexed<>(
          raft.getLogWriter().getLastIndex(),
          RaftLogEntry.newBuilder()
              .setTerm(raft.getTerm())
              .setTimestamp(System.currentTimeMillis())
              .setQuery(QueryEntry.newBuilder()
                  .setValue(request.getValue())
                  .build())
              .build(), 0);

      return applyQuery(entry, handler);
    } else {
      return queryForward(request, handler);
    }
  }

  /**
   * Forwards the query to the leader.
   */
  private CompletableFuture<QueryResponse> queryForward(QueryRequest request) {
    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.NO_LEADER)
          .build()));
    }

    log.trace("Forwarding {}", request);
    return forward(request, raft.getProtocol()::query)
        .exceptionally(error -> QueryResponse.newBuilder()
            .setStatus(ResponseStatus.ERROR)
            .setError(RaftError.NO_LEADER)
            .build())
        .thenApply(this::logResponse);
  }

  /**
   * Forwards the query to the leader.
   */
  private CompletableFuture<Void> queryForward(QueryRequest request, StreamHandler<QueryResponse> handler) {
    DefaultRaftMember leader = raft.getLeader();
    if (leader == null) {
      handler.next(QueryResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.NO_LEADER)
          .build());
      handler.complete();
      return CompletableFuture.completedFuture(null);
    } else {
      log.trace("Forwarding {}", request);
      return raft.getProtocol().queryStream(leader.memberId(), request, handler);
    }
  }

  /**
   * Performs a local query.
   */
  protected CompletableFuture<QueryResponse> queryLocal(Indexed<RaftLogEntry> entry) {
    return applyQuery(entry);
  }

  /**
   * Performs a local query.
   */
  protected CompletableFuture<Void> queryLocal(Indexed<RaftLogEntry> entry, StreamHandler<QueryResponse> handler) {
    return applyQuery(entry, handler);
  }

  /**
   * Applies a query to the state machine.
   */
  protected CompletableFuture<QueryResponse> applyQuery(Indexed<RaftLogEntry> entry) {
    // In the case of the leader, the state machine is always up to date, so no queries will be queued and all query
    // indexes will be the last applied index.
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    raft.getServiceManager().<OperationResult>apply(entry).whenComplete((result, error) -> {
      completeQuery(result, QueryResponse.newBuilder(), error, future);
    });
    return future;
  }

  protected CompletableFuture<Void> applyQuery(Indexed<RaftLogEntry> entry, StreamHandler<QueryResponse> handler) {
    return raft.getServiceManager().apply(entry, new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        handler.next(QueryResponse.newBuilder()
            .setStatus(ResponseStatus.OK)
            .setOutput(ByteString.copyFrom(value))
            .build());
      }

      @Override
      public void complete() {
        handler.complete();
      }

      @Override
      public void error(Throwable error) {
        if (error instanceof RaftException) {
          handler.next(QueryResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(((RaftException) error).getType())
              .build());
          handler.complete();
        } else if (error instanceof CompletionException && error.getCause() instanceof RaftException) {
          handler.next(QueryResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(((RaftException) error.getCause()).getType())
              .build());
          handler.complete();
        } else {
          log.warn("An unexpected error occurred: {}", error);
          handler.next(QueryResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(RaftError.PROTOCOL_ERROR)
              .build());
          handler.complete();
        }
      }
    });
  }

  /**
   * Completes an operation.
   */
  protected void completeQuery(OperationResult result, QueryResponse.Builder builder, Throwable error, CompletableFuture<QueryResponse> future) {
    if (result != null) {
      if (result.failed()) {
        error = result.error();
      }
    }

    if (error == null) {
      if (result == null) {
        future.complete(builder.setStatus(ResponseStatus.ERROR)
            .setError(RaftError.PROTOCOL_ERROR)
            .build());
      } else {
        future.complete(builder.setStatus(ResponseStatus.OK)
            .setOutput(result.result() != null ? ByteString.copyFrom(result.result()) : ByteString.EMPTY)
            .build());
      }
    } else if (error instanceof CompletionException && error.getCause() instanceof RaftException) {
      future.complete(builder.setStatus(ResponseStatus.ERROR)
          .setError(((RaftException) error.getCause()).getType())
          .setMessage(error.getMessage())
          .build());
    } else if (error instanceof RaftException) {
      future.complete(builder.setStatus(ResponseStatus.ERROR)
          .setError(((RaftException) error).getType())
          .setMessage(error.getMessage())
          .build());
    } else {
      log.warn("An unexpected error occurred: {}", error);
      future.complete(builder.setStatus(ResponseStatus.ERROR)
          .setError(RaftError.PROTOCOL_ERROR)
          .setMessage(error.getMessage())
          .build());
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.getTerm(), request.getLeader());

    log.debug("Received snapshot {} chunk from {}", request.getIndex(), request.getLeader());

    // If the request is for a lesser term, reject the request.
    if (request.getTerm() < raft.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.ILLEGAL_MEMBER_STATE)
          .build()));
    }

    // If the index has already been applied, we have enough state to populate the state machine up to this index.
    // Skip the snapshot and response successfully.
    if (raft.getLastApplied() > request.getIndex()) {
      return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .build()));
    }

    // If the snapshot already exists locally, do not overwrite it with a replicated snapshot. Simply reply to the
    // request successfully.
    Snapshot existingSnapshot = raft.getSnapshotStore().getSnapshot(request.getIndex());
    if (existingSnapshot != null) {
      return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .build()));
    }

    // If a snapshot is currently being received and the snapshot versions don't match, simply
    // close the existing snapshot. This is a naive implementation that assumes that the leader
    // will be responsible in sending the correct snapshot to this server. Leaders must dictate
    // where snapshots must be sent since entries can still legitimately exist prior to the snapshot,
    // and so snapshots aren't simply sent at the beginning of the follower's log, but rather the
    // leader dictates when a snapshot needs to be sent.
    if (pendingSnapshot != null && request.getIndex() != pendingSnapshot.snapshot().index()) {
      log.debug("Rolling back snapshot {}", pendingSnapshot.snapshot().index());
      pendingSnapshot.rollback();
      pendingSnapshot = null;
    }

    // If there is no pending snapshot, create a new snapshot.
    if (pendingSnapshot == null) {
      Snapshot snapshot = raft.getSnapshotStore().newSnapshot(
          request.getIndex(),
          WallClockTimestamp.from(request.getTimestamp()));
      pendingSnapshot = new PendingSnapshot(snapshot);
    }

    // Write the data to the snapshot.
    try (OutputStream output = pendingSnapshot.snapshot().openOutputStream()) {
      output.write(request.getData().toByteArray());
    } catch (IOException e) {
      log.warn("Failed to open snapshot", e);
    }

    // If the snapshot is complete, store the snapshot and reset state, otherwise update the next snapshot offset.
    if (request.getComplete()) {
      log.debug("Committing snapshot {}", pendingSnapshot.snapshot().index());
      pendingSnapshot.commit();
      pendingSnapshot = null;
    } else {
      pendingSnapshot.incrementOffset();
    }

    return CompletableFuture.completedFuture(logResponse(InstallResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .build()));
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request) {
    raft.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.ILLEGAL_MEMBER_STATE)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.getTerm(), null);

    return CompletableFuture.completedFuture(logResponse(VoteResponse.newBuilder()
        .setStatus(ResponseStatus.ERROR)
        .setError(RaftError.ILLEGAL_MEMBER_STATE)
        .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::command)
          .exceptionally(error -> CommandResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(RaftError.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<Void> onCommand(CommandRequest request, StreamHandler<CommandResponse> handler) {
    raft.checkThread();
    logRequest(request);

    DefaultRaftMember leader = raft.getLeader();
    if (leader == null) {
      handler.next(logResponse(CommandResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.NO_LEADER)
          .build()));
      handler.complete();
      return CompletableFuture.completedFuture(null);
    } else {
      log.trace("Forwarding {}", request);
      return raft.getProtocol().commandStream(leader.memberId(), request, handler);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(JoinRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::join)
          .exceptionally(error -> JoinResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(RaftError.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::reconfigure)
          .exceptionally(error -> ReconfigureResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(RaftError.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.NO_LEADER)
          .build()));
    } else {
      return forward(request, raft.getProtocol()::leave)
          .exceptionally(error -> LeaveResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(RaftError.NO_LEADER)
              .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (pendingSnapshot != null) {
      pendingSnapshot.rollback();
    }
    return super.stop();
  }

  /**
   * Pending snapshot.
   */
  private class PendingSnapshot {
    private final Snapshot snapshot;
    private long nextOffset;

    PendingSnapshot(Snapshot snapshot) {
      this.snapshot = snapshot;
    }

    /**
     * Returns the pending snapshot.
     *
     * @return the pending snapshot
     */
    public Snapshot snapshot() {
      return snapshot;
    }

    /**
     * Returns and increments the next snapshot offset.
     *
     * @return the next snapshot offset
     */
    public long nextOffset() {
      return nextOffset;
    }

    /**
     * Increments the next snapshot offset.
     */
    public void incrementOffset() {
      nextOffset++;
    }

    /**
     * Commits the snapshot to disk.
     */
    public void commit() {
      try {
        snapshot.complete();
      } catch (IOException e) {
        log.error("Failed to persist snapshot", e);
      }
    }

    /**
     * Closes and deletes the snapshot.
     */
    public void rollback() {
      snapshot.close();
      snapshot.delete();
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("snapshot", snapshot)
          .add("nextOffset", nextOffset)
          .toString();
    }
  }
}
