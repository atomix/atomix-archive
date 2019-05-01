/*
 * Copyright 2016-present Open Networking Foundation
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
 * limitations under the License
 */
package io.atomix.primitive.session;

import java.util.Collection;

import io.atomix.primitive.operation.StreamType;

/**
 * Provides session context for session-managed state machines.
 */
public interface Session {

  /**
   * Returns the session identifier.
   *
   * @return The session identifier.
   */
  SessionId sessionId();

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  State getState();

  /**
   * Returns a stream by ID.
   *
   * @param streamId the stream ID
   * @return the stream
   */
  default <T> SessionStreamHandler<T> getStream(long streamId) {
    return getStream(new StreamId(sessionId(), streamId));
  }

  /**
   * Returns a stream by ID.
   *
   * @param streamId the stream ID
   * @return the stream
   */
  <T> SessionStreamHandler<T> getStream(StreamId streamId);

  /**
   * Returns the collection of open streams of the given type.
   *
   * @return the collection of open streams of the given type
   */
  <T> Collection<SessionStreamHandler<T>> getStreams(StreamType<T> streamType);

  /**
   * Session state enums.
   */
  enum State {
    OPEN(true),
    SUSPICIOUS(true),
    EXPIRED(false),
    CLOSED(false);

    private final boolean active;

    State(boolean active) {
      this.active = active;
    }

    public boolean active() {
      return active;
    }
  }

}
