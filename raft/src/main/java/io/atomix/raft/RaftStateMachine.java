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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

/**
 * Raft state machine.
 */
public interface RaftStateMachine {

  /**
   * Takes a snapshot of the state machine.
   *
   * @param output the output
   */
  void snapshot(OutputStream output);

  /**
   * Installs a snapshot of the state machine.
   *
   * @param input the input
   */
  void install(InputStream input);

  /**
   * Returns whether the given index can be deleted.
   *
   * @param index the index to check
   * @return indicates whether the given index can be deleted
   */
  boolean canDelete(long index);

  /**
   * Applies the given write to the state machine.
   *
   * @param write the write to apply
   * @return the state machine output
   */
  CompletableFuture<byte[]> write(byte[] write);

  /**
   * Applies the given read to the state machine.
   *
   * @param read the read to apply
   * @return the state machine output
   */
  CompletableFuture<byte[]> read(byte[] read);

}
