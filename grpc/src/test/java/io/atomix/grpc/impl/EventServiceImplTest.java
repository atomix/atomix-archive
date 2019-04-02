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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.grpc.event.EventServiceGrpc;
import io.atomix.grpc.event.PublishRequest;
import io.atomix.grpc.event.PublishResponse;
import io.atomix.grpc.event.SubscribeRequest;
import io.atomix.grpc.event.SubscribeResponse;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Event service test.
 */
public class EventServiceImplTest extends GrpcServiceTest<EventServiceGrpc.EventServiceStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new EventServiceImpl(atomix);
  }

  @Override
  protected EventServiceGrpc.EventServiceStub getStub(Channel channel) {
    return EventServiceGrpc.newStub(channel);
  }

  @Test
  public void testEventService() throws Exception {
    EventServiceGrpc.EventServiceStub event1 = createStub();
    EventServiceGrpc.EventServiceStub event2 = createStub();
    EventServiceGrpc.EventServiceStub event3 = createStub();

    CountDownLatch latch = new CountDownLatch(2);
    AtomicInteger count1 = new AtomicInteger();
    event2.subscribe(SubscribeRequest.newBuilder().setTopic("test").build(), new StreamObserver<SubscribeResponse>() {
      @Override
      public void onNext(SubscribeResponse value) {
        assertArrayEquals("Hello world!".getBytes(), value.getPayload().toByteArray());
        count1.incrementAndGet();
        latch.countDown();
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });

    AtomicInteger count2 = new AtomicInteger();
    event3.subscribe(SubscribeRequest.newBuilder().setTopic("test").build(), new StreamObserver<SubscribeResponse>() {
      @Override
      public void onNext(SubscribeResponse value) {
        assertArrayEquals("Hello world!".getBytes(), value.getPayload().toByteArray());
        count2.incrementAndGet();
        latch.countDown();
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });

    StreamObserver<PublishRequest> stream = event1.publish(new StreamObserver<PublishResponse>() {
      @Override
      public void onNext(PublishResponse value) {

      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });

    Thread.sleep(1000);

    stream.onNext(PublishRequest.newBuilder()
        .setTopic("test")
        .setPayload(ByteString.copyFrom("Hello world!".getBytes()))
        .build());

    latch.await(5, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());
    assertEquals(1, count1.get());
    assertEquals(1, count2.get());
  }
}
