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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.operation.StreamType;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.impl.ServiceCodec;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionStreamHandler;
import io.atomix.primitive.session.StreamId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.misc.TimestampPrinter;
import org.slf4j.Logger;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primitive session.
 */
public class PrimitiveSession implements Session {
  private final Logger log;
  private final SessionId sessionId;
  private final long timeout;
  private final ServiceCodec codec;
  private final PrimitiveService.Context context;
  private volatile State state = State.CLOSED;
  private volatile long lastUpdated;
  private volatile long commandSequence;
  private volatile long commandLowWaterMark;
  private volatile long lastApplied;
  private final Map<Long, Runnable> sequenceCommands = new HashMap<>();
  private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();
  private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
  private final Map<Long, CompletableFuture<SessionCommandResponse>> results = new HashMap<>();
  private final PrimitiveStreamRegistry streams = new PrimitiveStreamRegistry();

  public PrimitiveSession(
      SessionId sessionId,
      long timeout,
      long lastUpdated,
      ServiceCodec codec,
      PrimitiveService.Context context) {
    this.sessionId = sessionId;
    this.timeout = timeout;
    this.lastUpdated = lastUpdated;
    this.codec = codec;
    this.context = context;
    this.log = context.getLogger();
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
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
   * Returns the session index.
   *
   * @return The session index.
   */
  public long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the session index.
   *
   * @param index The session index.
   */
  public void setLastApplied(long index) {
    // Query callbacks for this session are added to the indexQueries map to be executed once the required index
    // for the query is reached. For each increment of the index, trigger query callbacks that are dependent
    // on the specific index.
    for (long i = lastApplied + 1; i <= index; i++) {
      lastApplied = i;
      List<Runnable> queries = this.indexQueries.remove(lastApplied);
      if (queries != null) {
        for (Runnable query : queries) {
          query.run();
        }
      }
    }
  }

  /**
   * Registers a session index query.
   *
   * @param index The state machine index at which to execute the query.
   * @param query The query to execute.
   */
  public void registerIndexQuery(long index, Runnable query) {
    // Add a query to be run once the session's index reaches the given index.
    List<Runnable> queries = this.indexQueries.computeIfAbsent(index, v -> new LinkedList<>());
    queries.add(query);
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
    CompletableFuture<SessionCommandResponse> future = results.get(sequence);
    return future != null ? future : Futures.exceptionalFuture(new PrimitiveException.CommandFailure());
  }

  @Override
  public <T> PrimitiveSessionStream<T> getStream(long streamId) {
    return streams.getStream(streamId);
  }

  @Override
  public <T> PrimitiveSessionStream<T> getStream(StreamId streamId) {
    return streams.getStream(streamId.streamId());
  }

  /**
   * Returns the collection of all streams currently registered with the session.
   *
   * @return the collection of all streams currently registered with the session
   */
  @SuppressWarnings("unchecked")
  public Collection<PrimitiveSessionStream> getStreams() {
    return streams.getStreams();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Collection<SessionStreamHandler<T>> getStreams(StreamType<T> streamType) {
    return (Collection) streams.getStreams(streamType);
  }

  /**
   * Adds a stream to the session.
   *
   * @param streamId the stream ID
   * @param type     the stream type
   * @return the stream
   */
  public <T> PrimitiveSessionStream<T> addStream(long streamId, StreamType<T> type) {
    return new PrimitiveSessionStream<>(new StreamId(sessionId, streamId), type, codec.getStream(type), streams, context);
  }

  /**
   * Returns the last stream index.
   *
   * @return the last stream index
   */
  public long getStreamIndex() {
    return streams.getStreams()
        .stream()
        .mapToLong(stream -> stream.getLastIndex())
        .max()
        .orElse(context.getIndex());
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
    new ArrayList<>(streams.getStreams()).forEach(stream -> stream.complete());
  }

  /**
   * Closes the session.
   */
  public void close() {
    setState(State.CLOSED);
    new ArrayList<>(streams.getStreams()).forEach(stream -> stream.complete());
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
        .add("session", sessionId())
        .add("timestamp", TimestampPrinter.of(lastUpdated))
        .toString();
  }
}
