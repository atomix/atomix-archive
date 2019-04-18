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
package io.atomix.primitive.service;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.TestPrimitiveType;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.service.impl.DefaultCommit;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.session.Session;
import io.atomix.utils.time.WallClockTimestamp;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Default service executor test.
 */
public class DefaultServiceExecutorTest {
  @Test
  public void testExecuteOperations() throws Exception {
    ServiceExecutor executor = executor();
    Set<String> calls = new HashSet<>();

    executor.register(new DefaultOperationId("a", OperationType.COMMAND), () -> calls.add("a"));
    executor.<Void>register(new DefaultOperationId("b", OperationType.COMMAND), commit -> calls.add("b"), b -> null);
    executor.register(new DefaultOperationId("c", OperationType.QUERY), bytes -> {
      calls.add("c");
      return null;
    }, b -> b, b -> null);
    executor.register(new DefaultOperationId("d", OperationType.QUERY), () -> {
      calls.add("d");
      return null;
    }, b -> null);
    executor.register(new DefaultOperationId("e", OperationType.COMMAND), bytes -> {
      calls.add("e");
      return bytes;
    }, b -> b, b -> b);

    executor.apply(commit(new DefaultOperationId("a", OperationType.COMMAND), 1, null, System.currentTimeMillis()));
    assertTrue(calls.contains("a"));

    executor.apply(commit(new DefaultOperationId("b", OperationType.COMMAND), 2, null, System.currentTimeMillis()));
    assertTrue(calls.contains("b"));

    executor.apply(commit(new DefaultOperationId("c", OperationType.QUERY), 3, null, System.currentTimeMillis()));
    assertTrue(calls.contains("c"));

    executor.apply(commit(new DefaultOperationId("d", OperationType.QUERY), 4, null, System.currentTimeMillis()));
    assertTrue(calls.contains("d"));

    executor.apply(commit(new DefaultOperationId("e", OperationType.COMMAND), 5, null, System.currentTimeMillis()));
    assertTrue(calls.contains("e"));
  }

  @Test
  public void testScheduling() throws Exception {
    ServiceExecutor executor = executor();
    executor.register(new DefaultOperationId("a", OperationType.COMMAND), () -> {
    });
    executor.apply(commit(new DefaultOperationId("a", OperationType.COMMAND), 1, null, 0));

    Set<String> calls = new HashSet<>();
    executor.tick(new WallClockTimestamp(1));
    executor.schedule(Duration.ofMillis(100), () -> calls.add("a"));
    executor.tick(new WallClockTimestamp(100));
    assertFalse(calls.contains("a"));
    executor.tick(new WallClockTimestamp(101));
    assertTrue(calls.contains("a"));
  }

  private ServiceExecutor executor() {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.serviceType()).thenReturn(TestPrimitiveType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.currentOperation()).thenReturn(OperationType.COMMAND);
    return new DefaultServiceExecutor(context);
  }

  @SuppressWarnings("unchecked")
  private <T> Commit<T> commit(OperationId operation, long index, T value, long timestamp) {
    return new DefaultCommit<T>(index, operation, value, mock(Session.class), timestamp);
  }
}