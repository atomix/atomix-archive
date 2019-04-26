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
package io.atomix.raft;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import io.atomix.utils.stream.StreamHandler;
import org.slf4j.Logger;

/**
 * Raft state machine.
 */
public interface RaftStateMachine {

  /**
   * State machine context.
   */
  interface Context {

    /**
     * Returns the current state machine index.
     *
     * @return the current state machine index
     */
    long getIndex();

    /**
     * Returns the current state machine timestamp.
     *
     * @return the current state machine timestamp
     */
    long getTimestamp();

    /**
     * Returns the Raft operation type.
     *
     * @return the Raft operation type
     */
    RaftOperation.Type getOperationType();

    /**
     * Returns the state machine logger.
     *
     * @return the state machine logger
     */
    Logger getLogger();

  }

  /**
   * Initializes the state machine.
   *
   * @param context the state machine context
   */
  void init(Context context);

  /**
   * Takes a snapshot of the state machine.
   *
   * @param output the output
   */
  void snapshot(OutputStream output) throws IOException;

  /**
   * Installs a snapshot of the state machine.
   *
   * @param input the input
   */
  void install(InputStream input) throws IOException;

  /**
   * Returns whether the given index can be deleted.
   *
   * @param index the index to check
   * @return indicates whether the given index can be deleted
   */
  boolean canDelete(long index);

  /**
   * Applies the given command to the state machine.
   *
   * @param command the command to apply
   * @return the state machine output
   */
  CompletableFuture<byte[]> apply(RaftCommand command);

  /**
   * Applies the given command to the state machine.
   *
   * @param command the command to apply
   * @return the state machine output
   */
  CompletableFuture<Void> apply(RaftCommand command, StreamHandler<byte[]> handler);

  /**
   * Applies the given query to the state machine.
   *
   * @param query the query to apply
   * @return the state machine output
   */
  CompletableFuture<byte[]> apply(RaftQuery query);

  /**
   * Applies the given query to the state machine.
   *
   * @param query the query to apply
   * @return the state machine output
   */
  CompletableFuture<Void> apply(RaftQuery query, StreamHandler<byte[]> handler);

}
