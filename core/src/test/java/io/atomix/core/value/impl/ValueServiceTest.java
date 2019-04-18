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
package io.atomix.core.value.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.google.protobuf.ByteString;
import io.atomix.core.map.AtomicMapType;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.service.ServiceContext;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.time.WallClock;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Atomic value service test.
 */
public class ValueServiceTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSnapshot() throws Exception {
    ServiceContext context = mock(ServiceContext.class);
    when(context.serviceType()).thenReturn(AtomicMapType.instance());
    when(context.serviceName()).thenReturn("test");
    when(context.serviceId()).thenReturn(PrimitiveId.from(1));
    when(context.wallClock()).thenReturn(new WallClock());
    when(context.currentIndex()).thenReturn(1L);

    Session session = mock(Session.class);
    when(session.sessionId()).thenReturn(SessionId.from(1));

    ValueService service = new ValueService();
    service.init(context);

    assertTrue(service.get(GetRequest.newBuilder().build()).getValue().isEmpty());

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    service.backup(os);

    assertTrue(service.get(GetRequest.newBuilder().build()).getValue().isEmpty());

    service = new ValueService();
    service.init(context);
    service.restore(new ByteArrayInputStream(os.toByteArray()));

    assertTrue(service.get(GetRequest.newBuilder().build()).getValue().isEmpty());

    service.set(SetRequest.newBuilder().setValue(ByteString.copyFrom("Hello world!".getBytes())).build());
    assertArrayEquals("Hello world!".getBytes(), service.get(GetRequest.newBuilder().build()).getValue().toByteArray());

    os = new ByteArrayOutputStream();
    service.backup(os);

    assertArrayEquals("Hello world!".getBytes(), service.get(GetRequest.newBuilder().build()).getValue().toByteArray());

    service = new ValueService();
    service.init(context);
    service.restore(new ByteArrayInputStream(os.toByteArray()));

    assertArrayEquals("Hello world!".getBytes(), service.get(GetRequest.newBuilder().build()).getValue().toByteArray());

    service.set(null);
    assertTrue(service.get(GetRequest.newBuilder().build()).getValue().isEmpty());
  }
}
