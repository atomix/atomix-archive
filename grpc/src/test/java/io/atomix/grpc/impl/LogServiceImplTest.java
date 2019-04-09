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

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.grpc.log.ConsumeRequest;
import io.atomix.grpc.log.LogId;
import io.atomix.grpc.log.LogRecord;
import io.atomix.grpc.log.LogServiceGrpc;
import io.atomix.grpc.log.ProduceRequest;
import io.atomix.grpc.log.ProduceResponse;
import io.atomix.grpc.protocol.DistributedLogProtocol;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Distributed log service test.
 */
public class LogServiceImplTest extends GrpcServiceTest<LogServiceGrpc.LogServiceStub> {
  @Override
  protected BindableService getService(Atomix atomix) {
    return new LogServiceImpl(atomix);
  }

  @Override
  protected LogServiceGrpc.LogServiceStub getStub(Channel channel) {
    return LogServiceGrpc.newStub(channel);
  }

  @Test
  public void testLogService() throws Exception {
    LogServiceGrpc.LogServiceStub log1 = getStub(1);
    LogServiceGrpc.LogServiceStub log2 = getStub(2);

    LogId logId = LogId.newBuilder()
        .setLog(DistributedLogProtocol.newBuilder().build())
        .build();

    CountDownLatch latch = new CountDownLatch(1);
    log1.consume(ConsumeRequest.newBuilder()
        .setId(logId)
        .build(), new StreamObserver<LogRecord>() {
          @Override
          public void onNext(LogRecord value) {
            latch.countDown();
          }

          @Override
          public void onError(Throwable t) {

          }

          @Override
          public void onCompleted() {

          }
        });

    StreamObserver<ProduceRequest> produce = log2.produce(new StreamObserver<ProduceResponse>() {
      @Override
      public void onNext(ProduceResponse value) {

      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });

    produce.onNext(ProduceRequest.newBuilder()
        .setId(logId)
        .setValue(ByteString.copyFrom("Hello world!".getBytes()))
        .build());

    latch.await(5, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());
  }

  @Test
  public void testLogPartitionService() throws Exception {
    LogServiceGrpc.LogServiceStub log1 = getStub(1);
    LogServiceGrpc.LogServiceStub log2 = getStub(2);

    LogId logId = LogId.newBuilder()
        .setLog(DistributedLogProtocol.newBuilder().build())
        .build();

    CountDownLatch latch = new CountDownLatch(1);
    log1.consume(ConsumeRequest.newBuilder()
        .setId(logId)
        .setPartition(1)
        .build(), new StreamObserver<LogRecord>() {
          @Override
          public void onNext(LogRecord value) {
            latch.countDown();
          }

          @Override
          public void onError(Throwable t) {

          }

          @Override
          public void onCompleted() {

          }
        });

    StreamObserver<ProduceRequest> produce = log2.produce(new StreamObserver<ProduceResponse>() {
      @Override
      public void onNext(ProduceResponse value) {

      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });

    produce.onNext(ProduceRequest.newBuilder()
        .setId(logId)
        .setPartition(1)
        .setValue(ByteString.copyFrom("Hello world!".getBytes()))
        .build());

    latch.await(5, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());
  }
}
