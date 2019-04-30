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
package io.atomix.grpc.impl;

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.atomix.grpc.value.CheckAndSetRequest;
import io.atomix.grpc.value.GetRequest;
import io.atomix.grpc.value.SetRequest;
import io.atomix.grpc.value.ValueId;
import io.atomix.grpc.value.ValueServiceGrpc;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * gRPC value service test.
 */
public class ValueServiceImplTest extends GrpcServiceTest<ValueServiceGrpc.ValueServiceBlockingStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new ValueServiceImpl(atomix);
  }

  @Override
  protected ValueServiceGrpc.ValueServiceBlockingStub getStub(Channel channel) {
    return ValueServiceGrpc.newBlockingStub(channel);
  }

  @Test
  public void testGrpcValue() throws Exception {
    ValueServiceGrpc.ValueServiceBlockingStub value1 = getStub(1);
    ValueServiceGrpc.ValueServiceBlockingStub value2 = getStub(2);

    ValueId valueId = ValueId.newBuilder()
        .setName("test-value")
        .setRaft(MultiRaftProtocol.newBuilder().build())
        .build();

    assertEquals(0, value1.get(GetRequest.newBuilder()
        .setId(valueId)
        .build()).getVersion());

    assertTrue(value1.get(GetRequest.newBuilder()
        .setId(valueId)
        .build()).getValue().isEmpty());

    long version = value1.set(SetRequest.newBuilder()
        .setId(valueId)
        .setValue(ByteString.copyFrom("Hello world!".getBytes()))
        .build()).getVersion();

    assertTrue(version > 0);

    assertEquals(version, value2.get(GetRequest.newBuilder()
        .setId(valueId)
        .build()).getVersion());

    assertArrayEquals("Hello world!".getBytes(), value2.get(GetRequest.newBuilder()
        .setId(valueId)
        .build()).getValue().toByteArray());

    assertTrue(value2.checkAndSet(CheckAndSetRequest.newBuilder()
        .setId(valueId)
        .setVersion(version)
        .setUpdate(ByteString.copyFrom("Hello world again!".getBytes()))
        .build()).getSucceeded());

    assertArrayEquals("Hello world again!".getBytes(), value1.get(GetRequest.newBuilder()
        .setId(valueId)
        .build()).getValue().toByteArray());
  }
}
