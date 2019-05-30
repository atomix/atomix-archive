/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.client.map;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import io.atomix.client.AbstractPrimitiveTest;
import io.atomix.client.Versioned;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AtomicMap}.
 */
public class AtomicMapTest extends AbstractPrimitiveTest {

  @Test
  public void testBasicMapOperations() throws Throwable {
    final String fooValue = "Hello foo!";
    final String barValue = "Hello bar!";

    AtomicMap<String, String> map = client().<String, String>atomicMapBuilder("testBasicMapOperationMap").withSessionTimeout(Duration.ofSeconds(1))
        .build();

    assertTrue(map.isEmpty());
    assertNull(map.put("foo", fooValue));
    assertEquals(1, map.size());
    assertFalse(map.isEmpty());

    Versioned<String> value = map.putIfAbsent("foo", "Hello foo again!");
    assertNotNull(value);
    assertEquals(fooValue, value.value());

    assertNull(map.putIfAbsent("bar", barValue));
    assertEquals(2, map.size());

    assertEquals(2, map.keySet().size());

    assertEquals(2, map.values().size());
    List<String> rawValues = map.values().stream().map(v -> v.value()).collect(Collectors.toList());
    assertTrue(rawValues.contains("Hello foo!"));
    assertTrue(rawValues.contains("Hello bar!"));

    assertEquals(2, map.entrySet().size());

    assertEquals(fooValue, map.get("foo").value());
    assertEquals(fooValue, map.remove("foo").value());
    assertFalse(map.containsKey("foo"));
    assertNull(map.get("foo"));

    value = map.get("bar");
    assertNotNull(value);
    assertEquals(barValue, value.value());
    assertTrue(map.containsKey("bar"));
    assertEquals(1, map.size());

    value = map.replace("bar", "Goodbye bar!");
    assertNotNull(value);
    assertEquals(barValue, value.value());
    assertNull(map.replace("foo", "Goodbye foo!"));

    assertFalse(map.replace("foo", "Goodbye foo!", fooValue));
    assertTrue(map.replace("bar", "Goodbye bar!", barValue));
    assertFalse(map.replace("bar", "Goodbye bar!", barValue));

    value = map.get("bar");
    assertTrue(map.replace("bar", value.version(), "Goodbye bar!"));
    assertFalse(map.replace("bar", value.version(), barValue));

    map.clear();
    assertEquals(0, map.size());

    assertNull(map.put("foo", "Hello foo!", Duration.ofSeconds(3)));
    Thread.sleep(1000);
    assertEquals("Hello foo!", map.get("foo").value());
    Thread.sleep(5000);
    assertNull(map.get("foo"));

    assertNull(map.put("bar", "Hello bar!"));
    assertEquals("Hello bar!", map.put("bar", "Goodbye bar!", Duration.ofMillis(100)).value());
    assertEquals("Goodbye bar!", map.get("bar").value());
    Thread.sleep(5000);
    assertNull(map.get("bar"));

    assertNull(map.putIfAbsent("baz", "Hello baz!", Duration.ofMillis(100)));
    assertNotNull(map.get("baz"));
    Thread.sleep(5000);
    assertNull(map.get("baz"));
  }

  @Test
  public void testMapComputeOperations() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AtomicMap<String, String> map = client().<String, String>atomicMapBuilder("testMapComputeOperationsMap").build();

    assertEquals(value1, map.computeIfAbsent("foo", k -> value1).value());
    assertEquals(value1, map.computeIfAbsent("foo", k -> value2).value());
    assertNull(map.computeIfPresent("bar", (k, v) -> value2));
    assertEquals(value3, map.computeIfPresent("foo", (k, v) -> value3).value());
    assertNull(map.computeIfPresent("foo", (k, v) -> null));
    assertEquals(value1, map.computeIf("foo", v -> v == null, (k, v) -> value1).value());
    assertEquals(value2, map.compute("foo", (k, v) -> value2).value());
  }

  @Test
  public void testMapListeners() throws Throwable {
    final String value1 = "value1";
    final String value2 = "value2";
    final String value3 = "value3";

    AtomicMap<String, String> map = client().<String, String>atomicMapBuilder("testMapListenerMap").withSessionTimeout(Duration.ofSeconds(5))
        .build();
    TestAtomicMapEventListener listener = new TestAtomicMapEventListener();

    // add listener; insert new value into map and verify an INSERT event is received.
    map.addListener(listener);
    map.put("foo", value1);
    AtomicMapEvent<String, String> event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERTED, event.type());
    assertEquals(value1, event.newValue().value());

    // remove listener and verify listener is not notified.
    map.removeListener(listener);
    map.put("foo", value2);
    assertFalse(listener.eventReceived());

    // add the listener back and verify UPDATE events are received correctly
    map.addListener(listener);
    map.put("foo", value3);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.UPDATED, event.type());
    assertEquals(value3, event.newValue().value());

    // perform a non-state changing operation and verify no events are received.
    map.putIfAbsent("foo", value1);
    assertFalse(listener.eventReceived());

    // verify REMOVE events are received correctly.
    map.remove("foo");
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVED, event.type());
    assertEquals(value3, event.oldValue().value());

    // verify compute methods also generate events.
    map.computeIf("foo", v -> v == null, (k, v) -> value1);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERTED, event.type());
    assertEquals(value1, event.newValue().value());

    map.compute("foo", (k, v) -> value2);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.UPDATED, event.type());
    assertEquals(value2, event.newValue().value());

    map.computeIf("foo", v -> Objects.equals(v, value2), (k, v) -> null);
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVED, event.type());
    assertEquals(value2, event.oldValue().value());

    map.put("bar", "expire", Duration.ofSeconds(1));
    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.INSERTED, event.type());
    assertEquals("expire", event.newValue().value());

    event = listener.event();
    assertNotNull(event);
    assertEquals(AtomicMapEvent.Type.REMOVED, event.type());
    assertEquals("expire", event.oldValue().value());

    map.removeListener(listener);
  }

  @Test
  public void testMapViews() throws Exception {
    AtomicMap<String, String> map = client().<String, String>atomicMapBuilder("testMapViews").build();

    assertFalse(map.keySet().iterator().hasNext());
    assertFalse(map.entrySet().iterator().hasNext());
    assertFalse(map.values().iterator().hasNext());

    assertEquals(0, map.size());
    assertEquals(0, map.keySet().size());
    assertEquals(0, map.entrySet().size());
    assertEquals(0, map.values().size());

    assertTrue(map.isEmpty());
    assertTrue(map.keySet().isEmpty());
    assertTrue(map.entrySet().isEmpty());
    assertTrue(map.values().isEmpty());

    assertEquals(0, map.keySet().stream().count());
    assertEquals(0, map.entrySet().stream().count());
    assertEquals(0, map.values().stream().count());

    for (int i = 0; i < 100; i++) {
      map.put(String.valueOf(i), String.valueOf(i));
    }

    assertFalse(map.isEmpty());
    assertFalse(map.keySet().isEmpty());
    assertFalse(map.entrySet().isEmpty());
    assertFalse(map.values().isEmpty());

    assertEquals(100, map.keySet().stream().count());
    assertEquals(100, map.entrySet().stream().count());
    assertEquals(100, map.values().stream().count());

    String one = String.valueOf(1);
    String two = String.valueOf(2);
    String three = String.valueOf(3);
    String four = String.valueOf(4);

    assertTrue(map.keySet().contains(one));

    assertTrue(map.keySet().remove(one));
    assertFalse(map.keySet().contains(one));
    assertFalse(map.containsKey(one));

    assertTrue(map.entrySet().remove(Maps.immutableEntry(two, map.get(two))));
    assertFalse(map.keySet().contains(two));
    assertFalse(map.containsKey(two));

    assertTrue(map.entrySet().remove(Maps.immutableEntry(three, new Versioned<>(three, 0))));
    assertFalse(map.keySet().contains(three));
    assertFalse(map.containsKey(three));

    assertFalse(map.entrySet().remove(Maps.immutableEntry(four, new Versioned<>(four, 1))));
    assertTrue(map.keySet().contains(four));
    assertTrue(map.containsKey(four));

    assertEquals(97, map.size());
    assertEquals(97, map.keySet().size());
    assertEquals(97, map.entrySet().size());
    assertEquals(97, map.values().size());

    assertEquals(97, map.keySet().toArray().length);
    assertEquals(97, map.entrySet().toArray().length);
    assertEquals(97, map.values().toArray().length);

    assertEquals(97, map.keySet().toArray(new String[97]).length);
    assertEquals(97, map.entrySet().toArray(new Map.Entry[97]).length);
    assertEquals(97, map.values().toArray(new Versioned[97]).length);

    Iterator<String> iterator = map.keySet().iterator();
    int i = 0;
    while (iterator.hasNext()) {
      iterator.next();
      i += 1;
      map.put(String.valueOf(100 * i), String.valueOf(100 * i));
    }
    assertEquals(String.valueOf(100), map.get(String.valueOf(100)).value());
  }

  private static class TestAtomicMapEventListener implements AtomicMapEventListener<String, String> {
    private final BlockingQueue<AtomicMapEvent<String, String>> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(AtomicMapEvent<String, String> event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public AtomicMapEvent<String, String> event() throws InterruptedException {
      return queue.take();
    }
  }
}
