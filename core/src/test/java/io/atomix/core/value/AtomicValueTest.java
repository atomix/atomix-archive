/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.value;

import io.atomix.core.AbstractPrimitiveTest;
import io.atomix.utils.time.Versioned;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Raft atomic value test.
 */
public class AtomicValueTest extends AbstractPrimitiveTest {
  @Test
  public void testValue() throws Exception {
    AtomicValue<String> value = atomix().<String>atomicValueBuilder("test-value")
        .withProtocol(protocol())
        .build();
    assertNull(value.get().value());
    value.set("a");
    assertEquals("a", value.get().value());
    assertFalse(value.compareAndSet("b", "c").isPresent());
    assertTrue(value.compareAndSet("a", "b").isPresent());
    assertEquals("b", value.get().value());
    assertEquals("b", value.getAndSet("c").value());
    assertEquals("c", value.get().value());
  }

  @Test
  public void testEvents() throws Exception {
    AtomicValue<String> value1 = atomix().<String>atomicValueBuilder("test-value-events")
        .withProtocol(protocol())
        .build();
    AtomicValue<String> value2 = atomix().<String>atomicValueBuilder("test-value-events")
        .withProtocol(protocol())
        .build();

    BlockingAtomicValueListener<String> listener1 = new BlockingAtomicValueListener<>();
    BlockingAtomicValueListener<String> listener2 = new BlockingAtomicValueListener<>();

    value2.addListener(listener2);

    Versioned<String> value;
    AtomicValueEvent<String> event;

    value = value1.set("Hello world!");
    event = listener2.nextEvent();
    assertEquals("Hello world!", event.newValue().value());
    assertEquals(value.version(), event.newValue().version());

    value = value1.set("Hello world again!");
    event = listener2.nextEvent();
    assertEquals("Hello world again!", event.newValue().value());
    assertEquals(value.version(), event.newValue().version());

    value1.addListener(listener1);

    value = value2.set("Hello world back!");
    event = listener1.nextEvent();
    assertEquals("Hello world back!", event.newValue().value());
    assertEquals(value.version(), event.newValue().version());

    event = listener2.nextEvent();
    assertEquals("Hello world back!", event.newValue().value());
    assertEquals(value.version(), event.newValue().version());
  }

  private static class BlockingAtomicValueListener<T> implements AtomicValueEventListener<T> {
    private final BlockingQueue<AtomicValueEvent<T>> events = new LinkedBlockingQueue<>();

    @Override
    public void event(AtomicValueEvent<T> event) {
      events.add(event);
    }

    /**
     * Returns the next event.
     *
     * @return the next event
     */
    AtomicValueEvent<T> nextEvent() {
      try {
        return events.take();
      } catch (InterruptedException e) {
        return null;
      }
    }
  }
}
