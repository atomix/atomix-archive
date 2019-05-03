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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import io.atomix.raft.RaftException;
import io.atomix.raft.RaftServer;
import io.atomix.raft.cluster.impl.DefaultRaftMember;
import io.atomix.raft.cluster.impl.RaftMemberContext;
import io.atomix.raft.impl.OperationResult;
import io.atomix.raft.impl.RaftContext;
import io.atomix.raft.protocol.AppendRequest;
import io.atomix.raft.protocol.AppendResponse;
import io.atomix.raft.protocol.CommandRequest;
import io.atomix.raft.protocol.CommandResponse;
import io.atomix.raft.protocol.JoinRequest;
import io.atomix.raft.protocol.JoinResponse;
import io.atomix.raft.protocol.LeaveRequest;
import io.atomix.raft.protocol.LeaveResponse;
import io.atomix.raft.protocol.PollRequest;
import io.atomix.raft.protocol.PollResponse;
import io.atomix.raft.protocol.QueryRequest;
import io.atomix.raft.protocol.QueryResponse;
import io.atomix.raft.protocol.RaftError;
import io.atomix.raft.protocol.RaftMember;
import io.atomix.raft.protocol.ReconfigureRequest;
import io.atomix.raft.protocol.ReconfigureResponse;
import io.atomix.raft.protocol.ResponseStatus;
import io.atomix.raft.protocol.TransferRequest;
import io.atomix.raft.protocol.TransferResponse;
import io.atomix.raft.protocol.VoteRequest;
import io.atomix.raft.protocol.VoteResponse;
import io.atomix.raft.storage.log.CommandEntry;
import io.atomix.raft.storage.log.ConfigurationEntry;
import io.atomix.raft.storage.log.InitializeEntry;
import io.atomix.raft.storage.log.QueryEntry;
import io.atomix.raft.storage.log.RaftLogEntry;
import io.atomix.raft.storage.system.RaftConfiguration;
import io.atomix.storage.StorageException;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;

/**
 * Leader state.
 */
public final class LeaderRole extends ActiveRole {
  private static final int MAX_APPEND_ATTEMPTS = 5;

  private final LeaderAppender appender;
  private Scheduled appendTimer;
  private long configuring;
  private boolean transferring;

  public LeaderRole(RaftContext context) {
    super(context);
    this.appender = new LeaderAppender(this);
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.LEADER;
  }

  @Override
  public synchronized CompletableFuture<RaftRole> start() {
    // Reset state for the leader.
    takeLeadership();

    // Append initial entries to the log, including an initial no-op entry and the server's configuration.
    appendInitialEntries().join();

    // Commit the initial leader entries.
    commitInitialEntries();

    return super.start()
        .thenRun(this::startAppendTimer)
        .thenApply(v -> this);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    raft.setLeader(raft.getCluster().getMember().memberId());
    raft.getCluster().getRemoteMemberStates().forEach(m -> m.resetState(raft.getLog()));
  }

  /**
   * Appends initial entries to the log to take leadership.
   */
  private CompletableFuture<Void> appendInitialEntries() {
    final long term = raft.getTerm();
    return appendAndCompact(RaftLogEntry.newBuilder()
        .setTerm(term)
        .setTimestamp(appender.getTime())
        .setInitialize(InitializeEntry.newBuilder().build())
        .build())
        .thenApply(index -> null);
  }

  /**
   * Commits a no-op entry to the log, ensuring any entries from a previous term are committed.
   */
  private CompletableFuture<Void> commitInitialEntries() {
    // The Raft protocol dictates that leaders cannot commit entries from previous terms until
    // at least one entry from their current term has been stored on a majority of servers. Thus,
    // we force entries to be appended up to the leader's no-op entry. The LeaderAppender will ensure
    // that the commitIndex is not increased until the no-op entry (appender.index()) is committed.
    CompletableFuture<Void> future = new CompletableFuture<>();
    appender.appendEntries(appender.getIndex()).whenComplete((resultIndex, error) -> {
      raft.checkThread();
      if (isRunning()) {
        if (error == null) {
          raft.getServiceManager().apply(resultIndex);
          future.complete(null);
        } else {
          raft.setLeader(null);
          raft.transition(RaftServer.Role.FOLLOWER);
        }
      }
    });
    return future;
  }

  /**
   * Starts sending AppendEntries requests to all cluster members.
   */
  private void startAppendTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    log.trace("Starting append timer");
    appendTimer = raft.getThreadContext().schedule(Duration.ZERO, raft.getHeartbeatInterval(), this::appendMembers);
  }

  /**
   * Sends AppendEntries requests to members of the cluster that haven't heard from the leader in a while.
   */
  private void appendMembers() {
    raft.checkThread();
    if (isRunning()) {
      appender.appendEntries();
    }
  }

  /**
   * Returns a boolean value indicating whether a configuration is currently being committed.
   *
   * @return Indicates whether a configuration is currently being committed.
   */
  private boolean configuring() {
    return configuring > 0;
  }

  /**
   * Returns a boolean value indicating whether the leader is still being initialized.
   *
   * @return Indicates whether the leader is still being initialized.
   */
  private boolean initializing() {
    // If the leader index is 0 or is greater than the commitIndex, do not allow configuration changes.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    return appender.getIndex() == 0 || raft.getCommitIndex() < appender.getIndex();
  }

  /**
   * Commits the given configuration.
   */
  protected CompletableFuture<Long> configure(Collection<RaftMember> members) {
    raft.checkThread();

    final long term = raft.getTerm();
    return appendAndCompact(RaftLogEntry.newBuilder()
        .setTerm(term)
        .setTimestamp(System.currentTimeMillis())
        .setConfiguration(ConfigurationEntry.newBuilder()
            .addAllMembers(members)
            .build())
        .build())
        .thenComposeAsync(entry -> {
          // Store the index of the configuration entry in order to prevent other configurations from
          // being logged and committed concurrently. This is an important safety property of Raft.
          configuring = entry.index();
          raft.getCluster().configure(RaftConfiguration.newBuilder()
              .setIndex(entry.index())
              .setTerm(entry.entry().getTerm())
              .setTimestamp(entry.entry().getTimestamp())
              .addAllMembers(members)
              .build());

          return appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning() && commitError == null) {
              raft.getServiceManager().<OperationResult>apply(entry.index());
            }
            configuring = 0;
          });
        }, raft.getThreadContext());
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(final JoinRequest request) {
    raft.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .build()));
    }

    // If the member is already a known member of the cluster, complete the join successfully.
    if (raft.getCluster().getMember(request.getMember().getMemberId()) != null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .setIndex(raft.getCluster().getConfiguration().getIndex())
          .setTerm(raft.getCluster().getConfiguration().getTerm())
          .setTimestamp(raft.getCluster().getConfiguration().getTimestamp())
          .addAllMembers(raft.getCluster().getMembers().stream()
              .map(member -> RaftMember.newBuilder()
                  .setMemberId(member.memberId())
                  .setType(RaftMember.Type.valueOf(member.getType().name()))
                  .setUpdated(member.getLastUpdated().toEpochMilli())
                  .build())
              .collect(Collectors.toList()))
          .build()));
    }

    // Add the joining member to the members list. If the joining member's type is ACTIVE, join the member in the
    // PROMOTABLE state to allow it to get caught up without impacting the quorum size.
    Collection<RaftMember> members = raft.getCluster().getMembers().stream()
        .map(member -> RaftMember.newBuilder()
            .setMemberId(member.memberId())
            .setType(RaftMember.Type.valueOf(member.getType().name()))
            .setUpdated(member.getLastUpdated().toEpochMilli())
            .build())
        .collect(Collectors.toCollection(ArrayList::new));
    members.add(request.getMember());

    CompletableFuture<JoinResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      if (error == null) {
        future.complete(logResponse(JoinResponse.newBuilder()
            .setStatus(ResponseStatus.OK)
            .setIndex(index)
            .setTerm(raft.getCluster().getConfiguration().getTerm())
            .setTimestamp(raft.getCluster().getConfiguration().getTimestamp())
            .addAllMembers(members)
            .build()));
      } else {
        future.complete(logResponse(JoinResponse.newBuilder()
            .setStatus(ResponseStatus.ERROR)
            .setError(RaftError.PROTOCOL_ERROR)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(final ReconfigureRequest request) {
    raft.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the promote requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .build()));
    }

    // If the member is not a known member of the cluster, fail the promotion.
    DefaultRaftMember existingMember = raft.getCluster().getMember(request.getMember().getMemberId());
    if (existingMember == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.UNKNOWN_SESSION)
          .build()));
    }

    // If the configuration request index is less than the last known configuration index for
    // the leader, fail the request to ensure servers can't reconfigure an old configuration.
    if (request.getIndex() > 0 && request.getIndex() < raft.getCluster().getConfiguration().getIndex()
        || request.getTerm() != raft.getCluster().getConfiguration().getTerm()) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.CONFIGURATION_ERROR)
          .build()));
    }

    // If the member type has not changed, complete the configuration change successfully.
    if (existingMember.getType().name().equals(request.getMember().getType().name())) {
      RaftConfiguration configuration = raft.getCluster().getConfiguration();
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .setIndex(configuration.getIndex())
          .setTerm(configuration.getTerm())
          .setTimestamp(configuration.getTimestamp())
          .addAllMembers(configuration.getMembersList())
          .build()));
    }

    // Update the member type.
    existingMember.update(io.atomix.raft.cluster.RaftMember.Type.valueOf(request.getMember().getType().name()), Instant.now());

    Collection<RaftMember> members = raft.getCluster().getMembers().stream()
        .map(member -> RaftMember.newBuilder()
            .setMemberId(member.memberId())
            .setType(RaftMember.Type.valueOf(member.getType().name()))
            .setUpdated(member.getLastUpdated().toEpochMilli())
            .build())
        .collect(Collectors.toCollection(ArrayList::new));

    CompletableFuture<ReconfigureResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      if (error == null) {
        future.complete(logResponse(ReconfigureResponse.newBuilder()
            .setStatus(ResponseStatus.OK)
            .setIndex(index)
            .setTerm(raft.getCluster().getConfiguration().getTerm())
            .setTimestamp(raft.getCluster().getConfiguration().getTimestamp())
            .addAllMembers(members)
            .build()));
      } else {
        future.complete(logResponse(ReconfigureResponse.newBuilder()
            .setStatus(ResponseStatus.ERROR)
            .setError(RaftError.PROTOCOL_ERROR)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(final LeaveRequest request) {
    raft.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .build()));
    }

    Collection<RaftMember> members = raft.getCluster().getMembers().stream()
        .map(member -> RaftMember.newBuilder()
            .setMemberId(member.memberId())
            .setType(RaftMember.Type.valueOf(member.getType().name()))
            .setUpdated(member.getLastUpdated().toEpochMilli())
            .build())
        .collect(Collectors.toCollection(ArrayList::new));

    // If the leaving member is not a known member of the cluster, complete the leave successfully.
    if (raft.getCluster().getMember(request.getMember().getMemberId()) == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .addAllMembers(members)
          .build()));
    }

    RaftMember member = request.getMember();
    members.remove(member);

    CompletableFuture<LeaveResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      if (error == null) {
        future.complete(logResponse(LeaveResponse.newBuilder()
            .setStatus(ResponseStatus.OK)
            .setIndex(index)
            .setTerm(raft.getCluster().getConfiguration().getTerm())
            .setTimestamp(raft.getCluster().getConfiguration().getTimestamp())
            .addAllMembers(members)
            .build()));
      } else {
        future.complete(logResponse(LeaveResponse.newBuilder()
            .setStatus(ResponseStatus.ERROR)
            .setError(RaftError.PROTOCOL_ERROR)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<TransferResponse> onTransfer(final TransferRequest request) {
    logRequest(request);

    RaftMemberContext member = raft.getCluster().getMemberState(request.getMemberId());
    if (member == null) {
      return CompletableFuture.completedFuture(logResponse(TransferResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.ILLEGAL_MEMBER_STATE)
          .build()));
    }

    transferring = true;

    CompletableFuture<TransferResponse> future = new CompletableFuture<>();
    appender.appendEntries(raft.getLogWriter().getLastIndex()).whenComplete((result, error) -> {
      if (isRunning()) {
        if (error == null) {
          log.debug("Transferring leadership to {}", request.getMemberId());
          raft.transition(RaftServer.Role.FOLLOWER);
          future.complete(logResponse(TransferResponse.newBuilder()
              .setStatus(ResponseStatus.OK)
              .build()));
        } else if (error instanceof CompletionException && error.getCause() instanceof RaftException) {
          future.complete(logResponse(TransferResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(((RaftException) error.getCause()).getType())
              .build()));
        } else if (error instanceof RaftException) {
          future.complete(logResponse(TransferResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(((RaftException) error).getType())
              .build()));
        } else {
          future.complete(logResponse(TransferResponse.newBuilder()
              .setStatus(ResponseStatus.ERROR)
              .setError(RaftError.PROTOCOL_ERROR)
              .build()));
        }
      } else {
        future.complete(logResponse(TransferResponse.newBuilder()
            .setStatus(ResponseStatus.ERROR)
            .setError(RaftError.ILLEGAL_MEMBER_STATE)
            .build()));
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(final PollRequest request) {
    logRequest(request);

    // If a member sends a PollRequest to the leader, that indicates that it likely healed from
    // a network partition and may have had its status set to UNAVAILABLE by the leader. In order
    // to ensure heartbeats are immediately stored to the member, update its status if necessary.
    RaftMemberContext member = raft.getCluster().getMemberState(request.getCandidate());
    if (member != null) {
      member.resetFailureCount();
    }

    return CompletableFuture.completedFuture(logResponse(PollResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setTerm(raft.getTerm())
        .setAccepted(false)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(final VoteRequest request) {
    if (updateTermAndLeader(request.getTerm(), null)) {
      log.debug("Received greater term");
      raft.transition(RaftServer.Role.FOLLOWER);
      return super.onVote(request);
    } else {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(VoteResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .setTerm(raft.getTerm())
          .setVoted(false)
          .build()));
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    raft.checkThread();
    if (updateTermAndLeader(request.getTerm(), request.getLeader())) {
      CompletableFuture<AppendResponse> future = super.onAppend(request);
      raft.transition(RaftServer.Role.FOLLOWER);
      return future;
    } else if (request.getTerm() < raft.getTerm()) {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(AppendResponse.newBuilder()
          .setStatus(ResponseStatus.OK)
          .setTerm(raft.getTerm())
          .setSucceeded(false)
          .setLastLogIndex(raft.getLogWriter().getLastIndex())
          .build()));
    } else {
      raft.setLeader(request.getLeader());
      raft.transition(RaftServer.Role.FOLLOWER);
      return super.onAppend(request);
    }
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(final CommandRequest request) {
    raft.checkThread();
    logRequest(request);

    if (transferring) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.ILLEGAL_MEMBER_STATE)
          .build()));
    }

    final long term = raft.getTerm();
    final long timestamp = System.currentTimeMillis();

    CompletableFuture<CommandResponse> future = new CompletableFuture<CommandResponse>();
    appendAndCompact(RaftLogEntry.newBuilder()
        .setTerm(term)
        .setTimestamp(timestamp)
        .setCommand(CommandEntry.newBuilder()
            .setValue(request.getValue())
            .build())
        .build())
        .whenCompleteAsync((entry, appendError) -> {
          if (appendError != null) {
            Throwable cause = Throwables.getRootCause(appendError);
            if (Throwables.getRootCause(appendError) instanceof StorageException.TooLarge) {
              log.warn("Failed to append command {}", request, cause);
              future.complete(CommandResponse.newBuilder()
                  .setStatus(ResponseStatus.ERROR)
                  .setError(RaftError.PROTOCOL_ERROR)
                  .build());
            } else {
              future.complete(CommandResponse.newBuilder()
                  .setStatus(ResponseStatus.ERROR)
                  .setError(RaftError.COMMAND_FAILURE)
                  .build());
            }
            return;
          }

          // Replicate the command to followers.
          appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning()) {
              // If the command was successfully committed, apply it to the state machine.
              if (commitError == null) {
                raft.getServiceManager().<OperationResult>apply(entry.index()).whenComplete((result, error) -> {
                  if (result != null) {
                    if (result.failed()) {
                      error = result.error();
                    }
                  }

                  if (error == null) {
                    if (result == null) {
                      future.complete(CommandResponse.newBuilder()
                          .setStatus(ResponseStatus.ERROR)
                          .setError(RaftError.PROTOCOL_ERROR)
                          .build());
                    } else {
                      future.complete(CommandResponse.newBuilder()
                          .setStatus(ResponseStatus.OK)
                          .setOutput(result.result() != null ? ByteString.copyFrom(result.result()) : ByteString.EMPTY)
                          .build());
                    }
                  } else if (error instanceof CompletionException && error.getCause() instanceof RaftException) {
                    future.complete(CommandResponse.newBuilder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(((RaftException) error.getCause()).getType())
                        .setMessage(error.getMessage())
                        .build());
                  } else if (error instanceof RaftException) {
                    future.complete(CommandResponse.newBuilder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(((RaftException) error).getType())
                        .setMessage(error.getMessage())
                        .build());
                  } else {
                    log.warn("An unexpected error occurred: {}", error);
                    future.complete(CommandResponse.newBuilder()
                        .setStatus(ResponseStatus.ERROR)
                        .setError(RaftError.PROTOCOL_ERROR)
                        .setMessage(error.getMessage())
                        .build());
                  }
                });
              } else {
                future.complete(CommandResponse.newBuilder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.COMMAND_FAILURE)
                    .build());
              }
            } else {
              future.complete(CommandResponse.newBuilder()
                  .setStatus(ResponseStatus.ERROR)
                  .setError(RaftError.COMMAND_FAILURE)
                  .build());
            }
          });
        }, raft.getThreadContext());
    return future.thenApply(this::logResponse);
  }

  @Override
  public CompletableFuture<Void> onCommand(final CommandRequest request, StreamHandler<CommandResponse> handler) {
    raft.checkThread();
    logRequest(request);

    if (transferring) {
      handler.next(CommandResponse.newBuilder()
          .setStatus(ResponseStatus.ERROR)
          .setError(RaftError.ILLEGAL_MEMBER_STATE)
          .build());
      handler.complete();
      return CompletableFuture.completedFuture(null);
    }

    final long term = raft.getTerm();
    final long timestamp = System.currentTimeMillis();

    CompletableFuture<Void> future = new CompletableFuture<>();
    appendAndCompact(RaftLogEntry.newBuilder()
        .setTerm(term)
        .setTimestamp(timestamp)
        .setCommand(CommandEntry.newBuilder()
            .setValue(request.getValue())
            .setStream(true)
            .build())
        .build())
        .whenCompleteAsync((entry, appendError) -> {
          if (appendError != null) {
            Throwable cause = Throwables.getRootCause(appendError);
            if (Throwables.getRootCause(appendError) instanceof StorageException.TooLarge) {
              log.warn("Failed to append command {}", request, cause);
              handler.next(CommandResponse.newBuilder()
                  .setStatus(ResponseStatus.ERROR)
                  .setError(RaftError.PROTOCOL_ERROR)
                  .build());
              handler.complete();
            } else {
              handler.next(CommandResponse.newBuilder()
                  .setStatus(ResponseStatus.ERROR)
                  .setError(RaftError.COMMAND_FAILURE)
                  .build());
              handler.complete();
            }
            future.complete(null);
            return;
          }

          // Replicate the command to followers.
          appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
            raft.checkThread();
            if (isRunning()) {
              // If the command was successfully committed, apply it to the state machine.
              if (commitError == null) {
                raft.getServiceManager().<OperationResult>apply(entry.index(), new StreamHandler<byte[]>() {
                  @Override
                  public void next(byte[] value) {
                    handler.next(logResponse(CommandResponse.newBuilder()
                        .setStatus(ResponseStatus.OK)
                        .setOutput(ByteString.copyFrom(value))
                        .build()));
                  }

                  @Override
                  public void complete() {
                    handler.complete();
                  }

                  @Override
                  public void error(Throwable error) {
                    if (error instanceof RaftException) {
                      handler.next(logResponse(CommandResponse.newBuilder()
                          .setStatus(ResponseStatus.ERROR)
                          .setError(((RaftException) error).getType())
                          .build()));
                      handler.complete();
                    } else if (error instanceof CompletionException && error.getCause() instanceof RaftException) {
                      handler.next(logResponse(CommandResponse.newBuilder()
                          .setStatus(ResponseStatus.ERROR)
                          .setError(((RaftException) error.getCause()).getType())
                          .build()));
                      handler.complete();
                    } else {
                      log.warn("An unexpected error occurred: {}", error);
                      handler.next(logResponse(CommandResponse.newBuilder()
                          .setStatus(ResponseStatus.ERROR)
                          .setError(RaftError.PROTOCOL_ERROR)
                          .build()));
                      handler.complete();
                    }
                  }
                }).whenComplete((result, error) -> future.complete(null));
              } else {
                handler.next(logResponse(CommandResponse.newBuilder()
                    .setStatus(ResponseStatus.ERROR)
                    .setError(RaftError.COMMAND_FAILURE)
                    .build()));
                handler.complete();
                future.complete(null);
              }
            } else {
              handler.next(logResponse(CommandResponse.newBuilder()
                  .setStatus(ResponseStatus.ERROR)
                  .setError(RaftError.COMMAND_FAILURE)
                  .build()));
              handler.complete();
              future.complete(null);
            }
          });
        }, raft.getThreadContext());
    return future;
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(final QueryRequest request) {
    raft.checkThread();
    logRequest(request);

    final Indexed<RaftLogEntry> entry = new Indexed<>(
        raft.getLogWriter().getLastIndex(),
        RaftLogEntry.newBuilder()
            .setTerm(raft.getTerm())
            .setTimestamp(System.currentTimeMillis())
            .setQuery(QueryEntry.newBuilder()
                .setValue(request.getValue())
                .build())
            .build(), 0);

    final CompletableFuture<QueryResponse> future;
    switch (request.getReadConsistency()) {
      case SEQUENTIAL:
        future = queryLocal(entry);
        break;
      case LINEARIZABLE_LEASE:
        future = queryBoundedLinearizable(entry);
        break;
      case LINEARIZABLE:
        future = queryLinearizable(entry);
        break;
      default:
        future = Futures.exceptionalFuture(new IllegalStateException("Unknown consistency level: " + request.getReadConsistency()));
        break;
    }
    return future.thenApply(this::logResponse);
  }

  /**
   * Executes a bounded linearizable query.
   * <p>
   * Bounded linearizable queries succeed as long as this server remains the leader. This is possible
   * since the leader will step down in the event it fails to contact a majority of the cluster.
   */
  private CompletableFuture<QueryResponse> queryBoundedLinearizable(Indexed<RaftLogEntry> entry) {
    return applyQuery(entry);
  }

  /**
   * Executes a linearizable query.
   * <p>
   * Linearizable queries are first sequenced with commands and then applied to the state machine. Once
   * applied, we verify the node's leadership prior to responding successfully to the query.
   */
  private CompletableFuture<QueryResponse> queryLinearizable(Indexed<RaftLogEntry> entry) {
    return applyQuery(entry)
        .thenComposeAsync(response -> appender.appendEntries()
            .thenApply(index -> response)
            .exceptionally(error -> QueryResponse.newBuilder()
                .setStatus(ResponseStatus.ERROR)
                .setError(RaftError.QUERY_FAILURE)
                .setMessage(error.getMessage())
                .build()), raft.getThreadContext());
  }

  @Override
  public CompletableFuture<Void> onQuery(final QueryRequest request, StreamHandler<QueryResponse> handler) {
    raft.checkThread();
    logRequest(request);

    final Indexed<RaftLogEntry> entry = new Indexed<>(
        raft.getLogWriter().getLastIndex(),
        RaftLogEntry.newBuilder()
            .setTerm(raft.getTerm())
            .setTimestamp(System.currentTimeMillis())
            .setQuery(QueryEntry.newBuilder()
                .setValue(request.getValue())
                .build())
            .build(), 0);

    // No linearizability guarantee for stream queries.
    return queryLocal(entry, handler);
  }

  /**
   * Appends an entry to the Raft log and compacts logs if necessary.
   *
   * @param entry the entry to append
   * @param <E>   the entry type
   * @return a completable future to be completed once the entry has been appended
   */
  private <E extends RaftLogEntry> CompletableFuture<Indexed<E>> appendAndCompact(E entry) {
    return appendAndCompact(entry, 0);
  }

  /**
   * Appends an entry to the Raft log and compacts logs if necessary.
   *
   * @param entry   the entry to append
   * @param attempt the append attempt count
   * @param <E>     the entry type
   * @return a completable future to be completed once the entry has been appended
   */
  protected <E extends RaftLogEntry> CompletableFuture<Indexed<E>> appendAndCompact(E entry, int attempt) {
    if (attempt == MAX_APPEND_ATTEMPTS) {
      return Futures.exceptionalFuture(new StorageException.OutOfDiskSpace("Not enough space to append entry"));
    } else {
      try {
        return CompletableFuture.completedFuture(raft.getLogWriter().append(entry))
            .thenApply(indexed -> {
              log.trace("Appended {}", indexed);
              return indexed;
            });
      } catch (StorageException.TooLarge e) {
        return Futures.exceptionalFuture(e);
      } catch (StorageException.OutOfDiskSpace e) {
        log.warn("Caught OutOfDiskSpace error! Force compacting logs...");
        return raft.getServiceManager().compact().thenCompose(v -> appendAndCompact(entry, attempt + 1));
      }
    }
  }

  /**
   * Cancels the append timer.
   */
  private void cancelAppendTimer() {
    if (appendTimer != null) {
      log.trace("Cancelling append timer");
      appendTimer.cancel();
    }
  }

  /**
   * Ensures the local server is not the leader.
   */
  private void stepDown() {
    if (raft.getLeader() != null && raft.getLeader().equals(raft.getCluster().getMember())) {
      raft.setLeader(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> stop() {
    return super.stop()
        .thenRun(appender::close)
        .thenRun(this::cancelAppendTimer)
        .thenRun(this::stepDown);
  }
}
