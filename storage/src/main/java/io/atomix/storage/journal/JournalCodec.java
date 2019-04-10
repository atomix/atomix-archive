/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.storage.journal;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Journal codec.
 */
public interface JournalCodec<E> {

  /**
   * Encodes the given entry to the given buffer.
   *
   * @param entry the entry to encode
   * @param buffer the buffer to which to encode the entry
   */
  void encode(E entry, ByteBuffer buffer) throws IOException;

  /**
   * Decodes an entry from the given buffer.
   *
   * @param buffer the buffer from which to decode the entry
   * @return the decoded entry
   */
  E decode(ByteBuffer buffer) throws IOException;

}
