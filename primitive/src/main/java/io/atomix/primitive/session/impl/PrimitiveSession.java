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
package io.atomix.primitive.session.impl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.Role;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionServerProtocol;
import io.atomix.utils.misc.TimestampPrinter;
import org.slf4j.Logger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

/**
 * Primitive session.
 */
public class PrimitiveSession implements Session {
  private final Logger log;
  private final SessionId sessionId;
  private final MemberId memberId;
  private final long timeout;
  private final SessionServerProtocol protocol;
  private final PrimitiveService.Context context;
  private volatile State state = State.CLOSED;
  private volatile long lastUpdated;
  private volatile long commandSequence;
  private volatile long commandLowWaterMark;
  private volatile long eventIndex;
  private volatile long completeIndex;
  private final Map<Long, Runnable> sequenceCommands = new HashMap<>();
  private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
  private final Map<Long, CompletableFuture<SessionCommandResponse>> results = new HashMap<>();
  private final Queue<EventHolder> events = new LinkedList<>();
  private volatile EventHolder currentEventList;

  public PrimitiveSession(
      SessionId sessionId,
      MemberId memberId,
      long timeout,
      long lastUpdated,
      SessionServerProtocol protocol,
      PrimitiveService.Context context) {
    this.sessionId = sessionId;
    this.memberId = memberId;
    this.timeout = timeout;
    this.lastUpdated = lastUpdated;
    this.eventIndex = sessionId.id();
    this.completeIndex = sessionId.id();
    this.protocol = protocol;
    this.context = context;
    this.log = context.getLogger();
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
  }

  @Override
  public MemberId memberId() {
    return memberId;
  }

  /**
   * Returns the session timeout.
   *
   * @return the session timeout
   */
  public long timeout() {
    return timeout;
  }

  /**
   * Returns the session update timestamp.
   *
   * @return The session update timestamp.
   */
  public long getLastUpdated() {
    return lastUpdated;
  }

  /**
   * Updates the session timestamp.
   *
   * @param lastUpdated The session timestamp.
   */
  public void setLastUpdated(long lastUpdated) {
    this.lastUpdated = Math.max(this.lastUpdated, lastUpdated);
  }

  /**
   * Returns a boolean indicating whether the session is timed out.
   *
   * @param timestamp the current timestamp
   * @return indicates whether the session is timed out
   */
  public boolean isTimedOut(long timestamp) {
    long lastUpdated = this.lastUpdated;
    return lastUpdated > 0 && timestamp - lastUpdated > timeout;
  }

  @Override
  public State getState() {
    return state;
  }

  /**
   * Updates the session state.
   *
   * @param state The session state.
   */
  private void setState(State state) {
    if (this.state != state) {
      this.state = state;
      log.debug("State changed: {}", state);
    }
  }

  /**
   * Returns the session operation sequence number.
   *
   * @return The session operation sequence number.
   */
  public long getCommandSequence() {
    return commandSequence;
  }

  /**
   * Returns the next operation sequence number.
   *
   * @return The next operation sequence number.
   */
  public long nextCommandSequence() {
    return commandSequence + 1;
  }

  /**
   * Sets the session operation sequence number.
   *
   * @param sequence The session operation sequence number.
   */
  public void setCommandSequence(long sequence) {
    // For each increment of the sequence number, trigger query callbacks that are dependent on the specific sequence.
    for (long i = commandSequence + 1; i <= sequence; i++) {
      commandSequence = i;
      List<Runnable> queries = this.sequenceQueries.remove(commandSequence);
      if (queries != null) {
        for (Runnable query : queries) {
          query.run();
        }
      }
    }

    Runnable command = this.sequenceCommands.remove(nextCommandSequence());
    if (command != null) {
      command.run();
    }
  }

  /**
   * Registers a causal session query.
   *
   * @param sequence The session sequence number at which to execute the query.
   * @param query    The query to execute.
   */
  public void registerSequenceQuery(long sequence, Runnable query) {
    // Add a query to be run once the session's sequence number reaches the given sequence number.
    List<Runnable> queries = this.sequenceQueries.computeIfAbsent(sequence, v -> new LinkedList<>());
    queries.add(query);
  }

  /**
   * Registers a sequence command.
   *
   * @param sequence the sequence number
   * @param command  the command to execute
   */
  public void registerSequenceCommand(long sequence, Runnable command) {
    sequenceCommands.put(sequence, command);
  }

  /**
   * Registers a session result.
   * <p>
   * Results are stored in memory on all servers in order to provide linearizable semantics. When a command
   * is applied to the state machine, the command's return value is stored with the sequence number. Once the
   * client acknowledges receipt of the command output the result will be cleared from memory.
   *
   * @param sequence The result sequence number.
   * @param future   The result future.
   */
  public void registerResultFuture(long sequence, CompletableFuture<SessionCommandResponse> future) {
    results.put(sequence, future);
  }

  /**
   * Clears command results up to the given sequence number.
   * <p>
   * Command output is removed from memory up to the given sequence number. Additionally, since we know the
   * client received a response for all commands up to the given sequence number, command futures are removed
   * from memory as well.
   *
   * @param sequence The sequence to clear.
   */
  public void clearResults(long sequence) {
    if (sequence > commandLowWaterMark) {
      for (long i = commandLowWaterMark + 1; i <= sequence; i++) {
        results.remove(i);
        commandLowWaterMark = i;
      }
    }
  }

  /**
   * Returns the session response for the given sequence number.
   *
   * @param sequence The response sequence.
   * @return The response.
   */
  public CompletableFuture<SessionCommandResponse> getResultFuture(long sequence) {
    return results.get(sequence);
  }

  /**
   * Returns the session event index.
   *
   * @return The session event index.
   */
  public long getEventIndex() {
    return eventIndex;
  }

  /**
   * Sets the session event index.
   *
   * @param eventIndex the session event index
   */
  public void setEventIndex(long eventIndex) {
    this.eventIndex = eventIndex;
  }

  @Override
  public void publish(PrimitiveEvent event) {
    // Store volatile state in a local variable.
    State state = this.state;

    // If the sessions's state is not active, just ignore the event.
    if (!state.active()) {
      return;
    }

    // If the event is being published during a read operation, throw an exception.
    checkState(context.getOperationType() == OperationType.COMMAND, "session events can only be published during command execution");

    // If the client acked an index greater than the current event sequence number since we know the
    // client must have received it from another server.
    if (completeIndex > context.getIndex()) {
      return;
    }

    // If no event has been published for this index yet, create a new event holder.
    if (this.currentEventList == null || this.currentEventList.eventIndex != context.getIndex()) {
      long previousIndex = eventIndex;
      eventIndex = context.getIndex();
      this.currentEventList = new EventHolder(eventIndex, previousIndex);
    }

    // Add the event to the event holder.
    this.currentEventList.events.add(event);
  }

  /**
   * Commits events for the given index.
   */
  public void commit(long index) {
    if (currentEventList != null && currentEventList.eventIndex == index) {
      events.add(currentEventList);
      sendEvents(currentEventList);
      currentEventList = null;
    }
  }

  /**
   * Returns the index of the highest event acked for the session.
   *
   * @return The index of the highest event acked for the session.
   */
  public long getLastCompleted() {
    // If there are any queued events, return the index prior to the first event in the queue.
    EventHolder event = events.peek();
    if (event != null && event.eventIndex > completeIndex) {
      return event.eventIndex - 1;
    }
    return 0;
  }

  /**
   * Sets the last completed event index for the session.
   *
   * @param lastCompleted the last completed index
   */
  public void setLastCompleted(long lastCompleted) {
    this.completeIndex = lastCompleted;
  }

  /**
   * Clears events up to the given sequence.
   *
   * @param index The index to clear.
   */
  private void clearEvents(long index) {
    if (index > completeIndex) {
      EventHolder event = events.peek();
      while (event != null && event.eventIndex <= index) {
        events.remove();
        completeIndex = event.eventIndex;
        event = events.peek();
      }
      completeIndex = index;
    }
  }

  /**
   * Resends events from the given sequence.
   *
   * @param index The index from which to resend events.
   */
  public void resendEvents(long index) {
    clearEvents(index);
    for (EventHolder event : events) {
      sendEvents(event);
    }
  }

  /**
   * Sends an event to the session.
   */
  private void sendEvents(EventHolder event) {
    // Only send events to the client if this server is the leader.
    if (context.getRole() == Role.PRIMARY) {
      EventRequest request = EventRequest.newBuilder()
          .setSessionId(sessionId().id())
          .setEventIndex(event.eventIndex)
          .setPreviousIndex(event.previousIndex)
          .addAllEvents(event.events)
          .build();

      log.trace("Sending {}", request);
      protocol.event(memberId, request);
    }
  }

  /**
   * Opens the session.
   */
  public void open() {
    setState(State.OPEN);
  }

  /**
   * Expires the session.
   */
  public void expire() {
    setState(State.EXPIRED);
  }

  /**
   * Closes the session.
   */
  public void close() {
    setState(State.CLOSED);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), sessionId());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Session && ((Session) object).sessionId() == sessionId();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .addValue(context)
        .add("session", sessionId())
        .add("timestamp", TimestampPrinter.of(lastUpdated))
        .toString();
  }

  /**
   * Event holder.
   */
  private static class EventHolder {
    private final long eventIndex;
    private final long previousIndex;
    private final List<PrimitiveEvent> events = new LinkedList<>();

    private EventHolder(long eventIndex, long previousIndex) {
      this.eventIndex = eventIndex;
      this.previousIndex = previousIndex;
    }
  }

}
