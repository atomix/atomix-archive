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
package io.atomix.protocols.raft.session.impl;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.TestPrimitiveType;
import io.atomix.protocols.raft.protocol.OperationResponse;
import io.atomix.protocols.raft.protocol.PublishRequest;
import io.atomix.protocols.raft.protocol.ResponseStatus;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Client sequencer test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftSessionSequencerTest {

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventBeforeCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request = PublishRequest.newBuilder()
        .setSessionId(1)
        .setEventIndex(1)
        .setPreviousIndex(0)
        .addAllEvents(Collections.emptyList())
        .build();

    OperationResponse response = OperationResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setIndex(2)
        .setEventIndex(1)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceEvent(request, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAfterCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request = PublishRequest.newBuilder()
        .setSessionId(1)
        .setEventIndex(1)
        .setPreviousIndex(0)
        .addAllEvents(Collections.emptyList())
        .build();

    OperationResponse response = OperationResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setIndex(2)
        .setEventIndex(1)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceEvent(request, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAtCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request = PublishRequest.newBuilder()
        .setSessionId(1)
        .setEventIndex(2)
        .setPreviousIndex(0)
        .addAllEvents(Collections.emptyList())
        .build();

    OperationResponse response = OperationResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setIndex(2)
        .setEventIndex(2)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(1, run.getAndIncrement()));
    sequencer.sequenceEvent(request, () -> assertEquals(0, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAfterAllCommands() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence = sequencer.nextRequest();

    PublishRequest request1 = PublishRequest.newBuilder()
        .setSessionId(1)
        .setEventIndex(2)
        .setPreviousIndex(0)
        .addAllEvents(Collections.emptyList())
        .build();

    PublishRequest request2 = PublishRequest.newBuilder()
        .setSessionId(1)
        .setEventIndex(3)
        .setPreviousIndex(2)
        .addAllEvents(Collections.emptyList())
        .build();

    OperationResponse response = OperationResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setIndex(2)
        .setEventIndex(2)
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceEvent(request1, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceEvent(request2, () -> assertEquals(2, run.getAndIncrement()));
    sequencer.sequenceResponse(sequence, response, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(3, run.get());
  }

  /**
   * Tests sequencing an event that arrives before a command response.
   */
  @Test
  public void testSequenceEventAbsentCommand() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));

    PublishRequest request1 = PublishRequest.newBuilder()
        .setSessionId(1)
        .setEventIndex(2)
        .setPreviousIndex(0)
        .addAllEvents(Collections.emptyList())
        .build();

    PublishRequest request2 = PublishRequest.newBuilder()
        .setSessionId(1)
        .setEventIndex(3)
        .setPreviousIndex(2)
        .addAllEvents(Collections.emptyList())
        .build();

    AtomicInteger run = new AtomicInteger();
    sequencer.sequenceEvent(request1, () -> assertEquals(0, run.getAndIncrement()));
    sequencer.sequenceEvent(request2, () -> assertEquals(1, run.getAndIncrement()));
    assertEquals(2, run.get());
  }

  /**
   * Tests sequencing callbacks with the sequencer.
   */
  @Test
  public void testSequenceResponses() throws Throwable {
    RaftSessionSequencer sequencer = new RaftSessionSequencer(new RaftSessionState("test", SessionId.from(1), UUID.randomUUID().toString(), TestPrimitiveType.instance(), 1000));
    long sequence1 = sequencer.nextRequest();
    long sequence2 = sequencer.nextRequest();
    assertTrue(sequence2 == sequence1 + 1);

    OperationResponse commandResponse = OperationResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setIndex(2)
        .setEventIndex(0)
        .build();

    OperationResponse queryResponse = OperationResponse.newBuilder()
        .setStatus(ResponseStatus.OK)
        .setIndex(2)
        .setEventIndex(0)
        .build();

    AtomicBoolean run = new AtomicBoolean();
    sequencer.sequenceResponse(sequence2, queryResponse, () -> run.set(true));
    sequencer.sequenceResponse(sequence1, commandResponse, () -> assertFalse(run.get()));
    assertTrue(run.get());
  }
}
