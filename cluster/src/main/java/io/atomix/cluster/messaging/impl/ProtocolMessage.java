/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

/**
 * Base class for internal messages.
 */
public abstract class ProtocolMessage {

  /**
   * Internal message type.
   */
  public enum Type {
    REQUEST(1),
    REPLY(2),
    STREAM_REQUEST(3),
    STREAM_REPLY(4),
    STREAM(5),
    STREAM_END(6);

    private final int id;

    Type(int id) {
      this.id = id;
    }

    /**
     * Returns the unique message type ID.
     *
     * @return the unique message type ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the message type enum associated with the given ID.
     *
     * @param id the type ID.
     * @return the type enum for the given ID.
     */
    public static Type forId(int id) {
      switch (id) {
        case 1:
          return REQUEST;
        case 2:
          return REPLY;
        case 3:
          return STREAM_REQUEST;
        case 4:
          return STREAM_REPLY;
        case 5:
          return STREAM;
        case 6:
          return STREAM_END;
        default:
          throw new IllegalArgumentException("Unknown status ID " + id);
      }
    }
  }

  protected static final byte[] EMPTY_PAYLOAD = new byte[0];

  private final long id;

  protected ProtocolMessage(long id) {
    this.id = id;
  }

  /**
   * Returns the message type.
   *
   * @return the message type
   */
  public abstract Type type();

  /**
   * Returns the message ID.
   *
   * @return the message ID
   */
  public long id() {
    return id;
  }
}
