/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.lock.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import io.atomix.core.lock.AtomicLockType;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.time.WallClock;
import io.atomix.utils.time.WallClockTimestamp;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Leader elector service test.
 */
public class LockServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(AtomicLockType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.wallClock()).thenReturn(new WallClock());
    when(context.currentOperation()).thenReturn(OperationType.COMMAND);

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));
    when(context.currentSession()).thenReturn(session);

    LockService service = new LockService();
    service.init(context);
    service.register(session);
    service.tick(new WallClockTimestamp());

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    service.backup(os);

    service = new LockService();
    service.init(context);
    service.register(session);
    service.tick(new WallClockTimestamp());
    service.restore(new ByteArrayInputStream(os.toByteArray()));

    service.lock(LockRequest.newBuilder().setId(1).build());
    service.lock(LockRequest.newBuilder().setId(2).setTimeout(1000).build());

    os = new ByteArrayOutputStream();
    service.backup(os);

    service = new LockService();
    service.init(context);
    service.register(session);
    service.tick(new WallClockTimestamp());
    service.restore(new ByteArrayInputStream(os.toByteArray()));

    assertTrue(service.isLocked(IsLockedRequest.newBuilder().setIndex(service.lock.index).build()).getLocked());
    assertTrue(!service.queue.isEmpty());
    assertTrue(!service.timers.isEmpty());
  }
}
