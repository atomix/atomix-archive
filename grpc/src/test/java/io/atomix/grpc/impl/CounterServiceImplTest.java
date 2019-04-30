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

import io.atomix.core.Atomix;
import io.atomix.grpc.counter.CounterId;
import io.atomix.grpc.counter.CounterServiceGrpc;
import io.atomix.grpc.counter.DecrementRequest;
import io.atomix.grpc.counter.GetRequest;
import io.atomix.grpc.counter.IncrementRequest;
import io.atomix.grpc.counter.SetRequest;
import io.atomix.grpc.protocol.MultiRaftProtocol;
import io.grpc.BindableService;
import io.grpc.Channel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * gRPC counter service test.
 */
public class CounterServiceImplTest extends GrpcServiceTest<CounterServiceGrpc.CounterServiceBlockingStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new CounterServiceImpl(atomix);
  }

  @Override
  protected CounterServiceGrpc.CounterServiceBlockingStub getStub(Channel channel) {
    return CounterServiceGrpc.newBlockingStub(channel);
  }

  @Test
  public void testGrpcCounter() throws Exception {
    CounterServiceGrpc.CounterServiceBlockingStub counter1 = getStub(1);
    CounterServiceGrpc.CounterServiceBlockingStub counter2 = getStub(2);

    CounterId counterId = CounterId.newBuilder()
        .setName("test-counter")
        .setRaft(MultiRaftProtocol.newBuilder().build())
        .build();

    try {
      counter1.get(GetRequest.newBuilder().build());
      fail();
    } catch (Exception e) {
    }

    try {
      counter1.get(GetRequest.newBuilder()
          .setId(CounterId.newBuilder()
              .setName("foo")
              .build())
          .build());
      fail();
    } catch (Exception e) {
    }

    assertEquals(0, counter1.get(GetRequest.newBuilder().setId(counterId).build()).getValue());
    counter1.set(SetRequest.newBuilder()
        .setId(counterId)
        .setValue(1)
        .build());
    assertEquals(1, counter2.get(GetRequest.newBuilder().setId(counterId).build()).getValue());
    assertEquals(2, counter2.increment(IncrementRequest.newBuilder()
        .setId(counterId)
        .build())
        .getNextValue());
    assertEquals(4, counter2.increment(IncrementRequest.newBuilder()
        .setId(counterId)
        .setDelta(2)
        .build())
        .getNextValue());
    Thread.sleep(100);
    assertEquals(4, counter1.get(GetRequest.newBuilder().setId(counterId).build()).getValue());
    assertEquals(2, counter2.decrement(DecrementRequest.newBuilder()
        .setId(counterId)
        .setDelta(2)
        .build())
        .getNextValue());
  }
}
