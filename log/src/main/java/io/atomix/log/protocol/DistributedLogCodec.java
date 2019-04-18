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
package io.atomix.log.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.protobuf.CodedOutputStream;
import io.atomix.log.protocol.LogEntry;
import io.atomix.storage.journal.JournalCodec;

/**
 * Distributed log codec.
 */
public class DistributedLogCodec implements JournalCodec<LogEntry> {
  @Override
  public void encode(LogEntry entry, ByteBuffer buffer) throws IOException {
    CodedOutputStream stream = CodedOutputStream.newInstance(buffer);
    entry.writeTo(stream);
    stream.flush();
  }

  @Override
  public LogEntry decode(ByteBuffer buffer) throws IOException {
    LogEntry entry = LogEntry.parseFrom(buffer);
    buffer.position(buffer.limit());
    return entry;
  }
}
