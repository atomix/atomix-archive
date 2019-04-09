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
package io.atomix.core.workqueue.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collection;

import io.atomix.core.workqueue.Task;
import io.atomix.core.workqueue.WorkQueueType;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Work queue service test.
 */
public class DefaultWorkQueueServiceTest {
  @Test
  public void testSnapshot() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(WorkQueueType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));
    when(context.currentSession()).thenReturn(session);

    DefaultWorkQueueService service = new DefaultWorkQueueService();
    service.init(context);
    service.register(session);

    service.add(Arrays.asList("Hello world!".getBytes()));

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    service.backup(os);

    service = new DefaultWorkQueueService();
    service.init(context);
    service.register(session);
    service.restore(new ByteArrayInputStream(os.toByteArray()));

    Collection<Task<byte[]>> value = service.take(1);
    assertNotNull(value);
    assertEquals(1, value.size());
    assertArrayEquals("Hello world!".getBytes(), value.iterator().next().payload());
  }
}
