/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.primitive.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.service.Commit;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.Role;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.service.ServiceSession;
import io.atomix.primitive.service.ServiceSnapshot;
import io.atomix.primitive.service.StateMachine;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.LogicalClock;
import io.atomix.utils.time.LogicalTimestamp;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive service context.
 */
public class PrimitiveServiceContext implements ServiceContext {
  private final Logger log;
  private final MemberId localMemberId;
  private final PrimitiveId primitiveId;
  private final String serviceName;
  private final PrimitiveType primitiveType;
  private final PrimitiveService service;
  private final PrimitiveSessionRegistry sessions;
  private final ClusterCommunicationService communicationService;
  private final StateMachine.Context context;
  private final ThreadContext threadContext;
  private final ThreadContextFactory threadContextFactory;
  private long currentIndex;
  private Session currentSession;
  private long currentTimestamp;
  private long timestampDelta;
  private OperationType currentOperation;
  private boolean deleted;
  private final LogicalClock logicalClock = new LogicalClock() {
    @Override
    public LogicalTimestamp getTime() {
      return new LogicalTimestamp(currentIndex);
    }
  };
  private final WallClock wallClock = new WallClock() {
    @Override
    public WallClockTimestamp getTime() {
      return new WallClockTimestamp(currentTimestamp);
    }
  };

  public PrimitiveServiceContext(
      MemberId localMemberId,
      PrimitiveId primitiveId,
      String serviceName,
      PrimitiveType primitiveType,
      PrimitiveService service,
      PrimitiveSessionRegistry sessions,
      ClusterCommunicationService communicationService,
      StateMachine.Context context,
      ThreadContext threadContext,
      ThreadContextFactory threadContextFactory) {
    this.localMemberId = checkNotNull(localMemberId);
    this.primitiveId = checkNotNull(primitiveId);
    this.serviceName = checkNotNull(serviceName);
    this.primitiveType = checkNotNull(primitiveType);
    this.service = checkNotNull(service);
    this.sessions = checkNotNull(sessions);
    this.communicationService = checkNotNull(communicationService);
    this.context = checkNotNull(context);
    this.threadContext = threadContext;
    this.threadContextFactory = threadContextFactory;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .addValue(primitiveId)
        .add("type", primitiveType)
        .add("name", serviceName)
        .build());
    service.init(this);
  }

  /**
   * Returns a boolean indicating whether the service has been deleted.
   *
   * @return indicates whether the service has been deleted
   */
  public boolean deleted() {
    return deleted;
  }

  @Override
  public MemberId localMemberId() {
    return localMemberId;
  }

  public ThreadContext context() {
    return threadContext;
  }

  @Override
  public PrimitiveId serviceId() {
    return primitiveId;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  public Serializer serializer() {
    return service.serializer();
  }

  @Override
  public Role role() {
    return context.getRole();
  }

  @Override
  public long currentIndex() {
    return currentIndex;
  }

  @Override
  public Session currentSession() {
    return currentSession;
  }

  @Override
  public OperationType currentOperation() {
    return currentOperation;
  }

  @Override
  public LogicalClock logicalClock() {
    return logicalClock;
  }

  @Override
  public WallClock wallClock() {
    return wallClock;
  }

  /**
   * Sets the current state machine operation type.
   *
   * @param operation the current state machine operation type
   */
  private void setOperation(OperationType operation) {
    this.currentOperation = operation;
  }

  /**
   * Executes scheduled callbacks based on the provided time.
   */
  private void tick(long index, long timestamp) {
    this.currentIndex = index;

    // If the entry timestamp is less than the current state machine timestamp
    // and the delta is not yet set, set the delta and do not change the current timestamp.
    // If the entry timestamp is less than the current state machine timestamp
    // and the delta is set, update the current timestamp to the entry timestamp plus the delta.
    // If the entry timestamp is greater than or equal to the current timestamp, update the current
    // timestamp and reset the delta.
    if (timestamp < currentTimestamp) {
      if (timestampDelta == 0) {
        timestampDelta = currentTimestamp - timestamp;
      } else {
        currentTimestamp = timestamp + timestampDelta;
      }
    } else {
      currentTimestamp = timestamp;
      timestampDelta = 0;
    }

    // Set the current operation type to COMMAND to allow events to be sent.
    setOperation(OperationType.COMMAND);

    service.tick(WallClockTimestamp.from(timestamp));
  }

  /**
   * Expires sessions that have timed out.
   */
  private void expireSessions(long timestamp) {
    // Iterate through registered sessions.
    for (PrimitiveSession session : sessions.getSessions(primitiveId)) {
      if (session.isTimedOut(timestamp)) {
        log.debug("Session expired in {} milliseconds: {}", timestamp - session.getLastUpdated(), session);
        session = sessions.removeSession(session.sessionId());
        if (session != null) {
          session.expire();
          service.expire(session.sessionId());
        }
      }
    }
  }

  /**
   * Installs a snapshot.
   */
  public void installSnapshot(ServiceSnapshot snapshot) {
    log.debug("Installing snapshot {}", snapshot.getIndex());
    for (ServiceSession serviceSession : snapshot.getSessionsList()) {
      PrimitiveSession session = sessions.addSession(new PrimitiveSession(
          SessionId.from(serviceSession.getSessionId()),
          MemberId.from(serviceSession.getMemberId()),
          snapshot.getName(),
          primitiveType,
          serviceSession.getTimeout(),
          serviceSession.getTimestamp(),
          service.serializer(),
          communicationService,
          this,
          threadContextFactory));

      session.setRequestSequence(serviceSession.getRequestSequence());
      session.setCommandSequence(serviceSession.getCommandSequence());
      session.setEventIndex(serviceSession.getEventIndex());
      session.setLastCompleted(serviceSession.getLastCompleted());
      session.setLastApplied(snapshot.getIndex());
      session.setLastUpdated(serviceSession.getTimestamp());
      session.open();
      service.register(sessions.addSession(session));
    }

    try {
      service.restore(new ByteArrayInputStream(snapshot.getSnapshot().toByteArray()));
    } catch (IOException e) {
      log.error("Failed to restore service {}", snapshot.getName(), e);
    }
  }

  /**
   * Takes a snapshot of the service state.
   */
  public ServiceSnapshot takeSnapshot() {
    log.debug("Taking snapshot {}", currentIndex);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      service.backup(output);
    } catch (IOException e) {
      log.error("Failed to backup service {}", serviceName, e);
    }

    return ServiceSnapshot.newBuilder()
        .setId(primitiveId.id())
        .setType(primitiveType.name())
        .setName(serviceName)
        .setIndex(currentIndex)
        .setTimestamp(currentTimestamp)
        .setDelta(timestampDelta)
        .addAllSessions(sessions.getSessions().stream()
            .map(session -> ServiceSession.newBuilder()
                .setSessionId(session.sessionId().id())
                .setMemberId(session.memberId().id())
                .setTimeout(session.timeout())
                .setTimestamp(session.getLastUpdated())
                .setRequestSequence(session.getRequestSequence())
                .setCommandSequence(session.getCommandSequence())
                .setEventIndex(session.getEventIndex())
                .setLastApplied(session.getLastApplied())
                .build())
            .collect(Collectors.toList()))
        .setSnapshot(ByteString.copyFrom(output.toByteArray()))
        .build();
  }

  /**
   * Registers the given session.
   *
   * @param index     The index of the registration.
   * @param timestamp The timestamp of the registration.
   * @param session   The session to register.
   */
  public long openSession(long index, long timestamp, PrimitiveSession session) {
    log.debug("Opening session {}", session.sessionId());

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // Set the session timestamp to the current service timestamp.
    session.setLastUpdated(currentTimestamp);

    // Expire sessions that have timed out.
    expireSessions(currentTimestamp);

    // Add the session to the sessions list.
    session.open();
    service.register(sessions.addSession(session));

    // Commit the index, causing events to be sent to clients if necessary.
    commit();

    // Complete the future.
    return session.sessionId().id();
  }

  /**
   * Keeps the given session alive.
   *
   * @param index           The index of the keep-alive.
   * @param timestamp       The timestamp of the keep-alive.
   * @param session         The session to keep-alive.
   * @param commandSequence The session command sequence number.
   * @param eventIndex      The session event index.
   */
  public boolean keepAlive(long index, long timestamp, PrimitiveSession session, long commandSequence, long eventIndex) {
    // If the service has been deleted, just return false to ignore the keep-alive.
    if (deleted) {
      return false;
    }

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // The session may have been closed by the time this update was executed on the service thread.
    if (session.getState() != Session.State.CLOSED) {
      // Update the session's timestamp to prevent it from being expired.
      session.setLastUpdated(timestamp);

      // Clear results cached in the session.
      session.clearResults(commandSequence);

      // Resend missing events starting from the last received event index.
      session.resendEvents(eventIndex);

      // Update the session's request sequence number. The command sequence number will be applied
      // iff the existing request sequence number is less than the command sequence number. This must
      // be applied to ensure that request sequence numbers are reset after a leader change since leaders
      // track request sequence numbers in local memory.
      session.resetRequestSequence(commandSequence);

      // Update the sessions' command sequence number. The command sequence number will be applied
      // iff the existing sequence number is less than the keep-alive command sequence number. This should
      // not be the case under normal operation since the command sequence number in keep-alive requests
      // represents the highest sequence for which a client has received a response (the command has already
      // been completed), but since the log compaction algorithm can exclude individual entries from replication,
      // the command sequence number must be applied for keep-alive requests to reset the sequence number in
      // the event the last command for the session was cleaned/compacted from the log.
      session.setCommandSequence(commandSequence);

      // Expire sessions that have timed out.
      expireSessions(currentTimestamp);

      // Commit the index, causing events to be sent to clients if necessary.
      commit();

      // Complete the future.
      return true;
    } else {
      return false;
    }
  }

  /**
   * Keeps all sessions alive using the given timestamp.
   *
   * @param index     the index of the timestamp
   * @param timestamp the timestamp with which to reset session timeouts
   */
  public void keepAliveSessions(long index, long timestamp) {
    log.debug("Resetting session timeouts");

    this.currentIndex = index;
    this.currentTimestamp = Math.max(currentTimestamp, timestamp);

    for (PrimitiveSession session : sessions.getSessions(primitiveId)) {
      session.setLastUpdated(timestamp);
    }
  }

  /**
   * Unregister the given session.
   *
   * @param index     The index of the unregister.
   * @param timestamp The timestamp of the unregister.
   * @param session   The session to unregister.
   */
  public void closeSession(long index, long timestamp, PrimitiveSession session) {
    log.debug("Closing session {}", session.sessionId());

    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // Expire sessions that have timed out.
    expireSessions(currentTimestamp);

    // Remove the session from the sessions list.
    session = sessions.removeSession(session.sessionId());
    if (session != null) {
      session.close();
      service.close(session.sessionId());
    }

    // Commit the index, causing events to be sent to clients if necessary.
    commit();
  }

  /**
   * Executes the given command on the state machine.
   *
   * @param index     The index of the command.
   * @param timestamp The timestamp of the command.
   * @param sequence  The command sequence number.
   * @param session   The session that submitted the command.
   * @param operation The command to execute.
   * @return A future to be completed with the command result.
   */
  public CompletableFuture<OperationResult> executeCommand(long index, long sequence, long timestamp, PrimitiveSession session, PrimitiveOperation operation) {
    // If the service has been deleted then throw an unknown service exception.
    if (deleted) {
      log.warn("Service {} has been deleted by another process", serviceName);
      throw new PrimitiveException.UnknownService("Service " + serviceName + " has been deleted");
    }

    // If the session is not open, fail the request.
    if (!session.getState().active()) {
      log.warn("Session not open: {}", session);
      throw new PrimitiveException.UnknownSession("Unknown session: " + session.sessionId());
    }

    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(timestamp);

    // Update the state machine index/timestamp.
    tick(index, timestamp);

    // If the command's sequence number is less than the next session sequence number then that indicates that
    // we've received a command that was previously applied to the state machine. Ensure linearizability by
    // returning the cached response instead of applying it to the user defined state machine.
    CompletableFuture<OperationResult> future = new CompletableFuture<>();
    if (sequence > 0 && sequence < session.nextCommandSequence()) {
      log.trace("Returning cached result for command with sequence number {} < {}", sequence, session.nextCommandSequence());
      getResult(index, sequence, session, future);
    }
    // If the command's sequence number is greater than the next sequence number then that indicates some commands
    // are missing. Enqueue the command for replay later.
    else if (sequence > session.nextCommandSequence()) {
      log.trace("Registering command with sequence number " + sequence + " > " + session.nextCommandSequence());
      sequenceCommand(index, sequence, timestamp, session, operation, future);
    }
    // If we've made it this far, the command must have been applied in the proper order as sequenced by the
    // session. This should be the case for most commands applied to the state machine.
    else {
      // Execute the command in the state machine thread. Once complete, the CompletableFuture callback will be completed
      // in the state machine thread. Register the result in that thread and then complete the future in the caller's thread.
      applyCommand(index, sequence, timestamp, session, operation, future);
    }
    return future;
  }

  /**
   * Loads and returns a cached command result according to the sequence number.
   */
  private void getResult(long index, long sequence, PrimitiveSession session, CompletableFuture<OperationResult> future) {
    OperationResult result = session.getResult(sequence);
    if (result == null) {
      log.debug("Missing command result at index {}", index);
      future.completeExceptionally(new PrimitiveException.CommandFailure());
    } else {
      future.complete(result);
    }
  }

  /**
   * Enqueues the given command for in-order execution.
   */
  private void sequenceCommand(
      long index,
      long sequence,
      long timestamp,
      PrimitiveSession session,
      PrimitiveOperation operation,
      CompletableFuture<OperationResult> future) {
    session.registerSequenceCommand(sequence, () -> applyCommand(index, sequence, timestamp, session, operation, future));
  }

  /**
   * Applies the given commit to the state machine.
   */
  private void applyCommand(
      long index,
      long sequence,
      long timestamp,
      PrimitiveSession session,
      PrimitiveOperation operation,
      CompletableFuture<OperationResult> future) {
    long eventIndex = session.getEventIndex();

    Commit<byte[]> commit = new DefaultCommit<>(index, operation.getId(), operation.getValue().toByteArray(), session, timestamp);

    OperationResult result;
    try {
      currentSession = session;

      // Execute the state machine operation and get the result.
      byte[] output = service.apply(commit);

      // Store the result for linearizability and complete the command.
      result = OperationResult.succeeded(index, eventIndex, output);
    } catch (Exception e) {
      // If an exception occurs during execution of the command, store the exception.
      result = OperationResult.failed(index, eventIndex, e);
    } finally {
      currentSession = null;
    }

    // Once the operation has been applied to the state machine, commit events published by the command.
    // The state machine context will build a composite future for events published to all sessions.
    commit();

    // Register the result in the session to ensure retries receive the same output for the command.
    session.registerResult(sequence, result);

    // Update the session timestamp and command sequence number.
    session.setCommandSequence(sequence);

    // Complete the command.
    future.complete(result);
  }

  /**
   * Executes the given query on the state machine.
   *
   * @param index     The index of the query.
   * @param sequence  The query sequence number.
   * @param timestamp The timestamp of the query.
   * @param session   The session that submitted the query.
   * @param operation The query to execute.
   * @return A future to be completed with the query result.
   */
  public CompletableFuture<OperationResult> executeQuery(
      long index,
      long sequence,
      long timestamp,
      PrimitiveSession session,
      PrimitiveOperation operation) {
    CompletableFuture<OperationResult> future = new CompletableFuture<>();
    executeQuery(index, sequence, timestamp, session, operation, future);
    return future;
  }

  /**
   * Executes a query on the state machine thread.
   */
  private void executeQuery(
      long index,
      long sequence,
      long timestamp,
      PrimitiveSession session,
      PrimitiveOperation operation,
      CompletableFuture<OperationResult> future) {
    // If the service has been deleted then throw an unknown service exception.
    if (deleted) {
      log.warn("Service {} has been deleted by another process", serviceName);
      future.completeExceptionally(new PrimitiveException.UnknownService("Service " + serviceName + " has been deleted"));
      return;
    }

    // If the session is not open, fail the request.
    if (!session.getState().active()) {
      log.warn("Inactive session: " + session.sessionId());
      future.completeExceptionally(new PrimitiveException.UnknownSession("Unknown session: " + session.sessionId()));
      return;
    }

    // Otherwise, sequence the query.
    sequenceQuery(index, sequence, timestamp, session, operation, future);
  }

  /**
   * Sequences the given query.
   */
  private void sequenceQuery(
      long index,
      long sequence,
      long timestamp,
      PrimitiveSession session,
      PrimitiveOperation operation,
      CompletableFuture<OperationResult> future) {
    // If the query's sequence number is greater than the session's current sequence number, queue the request for
    // handling once the state machine is caught up.
    long commandSequence = session.getCommandSequence();
    if (sequence > commandSequence) {
      log.trace("Registering query with sequence number " + sequence + " > " + commandSequence);
      session.registerSequenceQuery(sequence, () -> indexQuery(index, timestamp, session, operation, future));
    } else {
      indexQuery(index, timestamp, session, operation, future);
    }
  }

  /**
   * Ensures the given query is applied after the appropriate index.
   */
  private void indexQuery(
      long index,
      long timestamp,
      PrimitiveSession session,
      PrimitiveOperation operation,
      CompletableFuture<OperationResult> future) {
    // If the query index is greater than the session's last applied index, queue the request for handling once the
    // state machine is caught up.
    if (index > currentIndex) {
      log.trace("Registering query with index " + index + " > " + currentIndex);
      session.registerIndexQuery(index, () -> applyQuery(timestamp, session, operation, future));
    } else {
      applyQuery(timestamp, session, operation, future);
    }
  }

  /**
   * Applies a query to the state machine.
   */
  private void applyQuery(long timestamp, PrimitiveSession session, PrimitiveOperation operation, CompletableFuture<OperationResult> future) {
    // If the service has been deleted then throw an unknown service exception.
    if (deleted) {
      log.warn("Service {} has been deleted by another process", serviceName);
      future.completeExceptionally(new PrimitiveException.UnknownService("Service " + serviceName + " has been deleted"));
      return;
    }

    // If the session is not open, fail the request.
    if (!session.getState().active()) {
      log.warn("Inactive session: " + session.sessionId());
      future.completeExceptionally(new PrimitiveException.UnknownSession("Unknown session: " + session.sessionId()));
      return;
    }

    // Set the current operation type to QUERY to prevent events from being sent to clients.
    setOperation(OperationType.QUERY);

    Commit<byte[]> commit = new DefaultCommit<>(currentIndex, operation.getId(), operation.getValue().toByteArray(), session, timestamp);

    long eventIndex = session.getEventIndex();

    OperationResult result;
    try {
      currentSession = session;
      result = OperationResult.succeeded(currentIndex, eventIndex, service.apply(commit));
    } catch (Exception e) {
      result = OperationResult.failed(currentIndex, eventIndex, e);
    } finally {
      currentSession = null;
    }
    future.complete(result);
  }

  /**
   * Commits the application of a command to the state machine.
   */
  @SuppressWarnings("unchecked")
  private void commit() {
    long index = this.currentIndex;
    for (PrimitiveSession session : sessions.getSessions(primitiveId)) {
      session.commit(index);
    }
  }

  /**
   * Closes the service context.
   */
  public void close() {
    for (PrimitiveSession serviceSession : sessions.getSessions(serviceId())) {
      serviceSession.close();
      service.close(serviceSession.sessionId());
    }
    service.close();
    deleted = true;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", primitiveType)
        .add("name", serviceName)
        .add("id", primitiveId)
        .toString();
  }
}
