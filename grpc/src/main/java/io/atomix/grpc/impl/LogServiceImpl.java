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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.core.log.AsyncDistributedLog;
import io.atomix.core.log.Record;
import io.atomix.grpc.log.ConsumeRequest;
import io.atomix.grpc.log.LogId;
import io.atomix.grpc.log.LogRecord;
import io.atomix.grpc.log.LogServiceGrpc;
import io.atomix.grpc.log.ProduceRequest;
import io.atomix.grpc.log.ProduceResponse;
import io.atomix.primitive.protocol.LogProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.utils.concurrent.Futures;
import io.grpc.stub.StreamObserver;

/**
 * Log service implementation.
 */
public class LogServiceImpl extends LogServiceGrpc.LogServiceImplBase {
  private final Atomix atomix;

  public LogServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private LogProtocol toProtocol(LogId id) {
    return DistributedLogProtocol.builder(id.getLog().getGroup())
        .build();
  }

  private CompletableFuture<AsyncDistributedLog<byte[]>> getLog(LogId id) {
    return atomix.<byte[]>logBuilder()
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(log -> log.async());
  }

  @Override
  public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responseObserver) {
    Map<LogId, CompletableFuture<AsyncDistributedLog<byte[]>>> logs = new ConcurrentHashMap<>();
    return new StreamObserver<ProduceRequest>() {
      @Override
      public void onNext(ProduceRequest request) {
        logs.computeIfAbsent(request.getId(), id -> getLog(id))
            .whenComplete((log, error) -> {
              if (error == null) {
                log.produce(request.getValue().toByteArray());
              } else {
                responseObserver.onError(error);
              }
            });
      }

      @Override
      public void onError(Throwable t) {
        Futures.allOf(logs.values().stream())
            .thenAccept(logs -> Futures.allOf(logs.map(AsyncDistributedLog::close)))
            .whenComplete((r, e) -> responseObserver.onCompleted());
      }

      @Override
      public void onCompleted() {
        Futures.allOf(logs.values().stream())
            .thenAccept(logs -> Futures.allOf(logs.map(AsyncDistributedLog::close)))
            .whenComplete((r, e) -> responseObserver.onCompleted());
      }
    };
  }

  @Override
  public StreamObserver<ConsumeRequest> consume(StreamObserver<LogRecord> responseObserver) {
    Map<LogId, CompletableFuture<AsyncDistributedLog<byte[]>>> logs = new ConcurrentHashMap<>();
    return new StreamObserver<ConsumeRequest>() {
      @Override
      public void onNext(ConsumeRequest request) {
        Consumer<Record<byte[]>> consumer = record -> {
          responseObserver.onNext(LogRecord.newBuilder()
              .setOffset(record.offset())
              .setTimestamp(record.timestamp())
              .setValue(ByteString.copyFrom(record.value()))
              .build());
        };

        logs.computeIfAbsent(request.getId(), id -> getLog(id))
            .whenComplete((log, error) -> {
              if (error == null) {
                if (request.getPartition() == 0) {
                  log.consume(consumer);
                } else {
                  log.getPartition(request.getPartition()).consume(consumer);
                }
              } else {
                responseObserver.onError(error);
              }
            });
      }

      @Override
      public void onError(Throwable t) {
        Futures.allOf(logs.values().stream())
            .thenAccept(logs -> Futures.allOf(logs.map(AsyncDistributedLog::close)))
            .whenComplete((r, e) -> responseObserver.onCompleted());
      }

      @Override
      public void onCompleted() {
        Futures.allOf(logs.values().stream())
            .thenAccept(logs -> Futures.allOf(logs.map(AsyncDistributedLog::close)))
            .whenComplete((r, e) -> responseObserver.onCompleted());
      }
    };
  }
}
