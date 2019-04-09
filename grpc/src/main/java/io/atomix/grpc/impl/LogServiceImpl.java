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
import io.atomix.core.log.DistributedLog;
import io.atomix.core.log.DistributedLogType;
import io.atomix.core.log.Record;
import io.atomix.grpc.log.ConsumeRequest;
import io.atomix.grpc.log.LogId;
import io.atomix.grpc.log.LogRecord;
import io.atomix.grpc.log.LogServiceGrpc;
import io.atomix.grpc.log.ProduceRequest;
import io.atomix.grpc.log.ProduceResponse;
import io.atomix.utils.concurrent.Futures;
import io.grpc.stub.StreamObserver;

/**
 * Log service implementation.
 */
public class LogServiceImpl extends LogServiceGrpc.LogServiceImplBase {
  private final PrimitiveExecutor<DistributedLog<byte[]>, AsyncDistributedLog<byte[]>> executor;

  public LogServiceImpl(Atomix atomix) {
    this.executor = new PrimitiveExecutor<>(atomix, DistributedLogType.instance(), DistributedLog::async);
  }

  @Override
  public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responseObserver) {
    Map<LogId, CompletableFuture<AsyncDistributedLog<byte[]>>> logs = new ConcurrentHashMap<>();
    return new StreamObserver<ProduceRequest>() {
      @Override
      public void onNext(ProduceRequest request) {
        if (executor.isValidRequest(request, ProduceResponse::getDefaultInstance, responseObserver)) {
          logs.computeIfAbsent(request.getId(), executor::getPrimitive)
              .whenComplete((log, error) -> {
                if (error == null) {
                  if (request.getPartition() == 0) {
                    log.produce(request.getValue().toByteArray())
                        .whenComplete((produceResult, produceError) -> {
                          if (produceError != null) {
                            responseObserver.onError(produceError);
                          }
                        });
                  } else {
                    log.getPartition(request.getPartition()).produce(request.getValue().toByteArray())
                        .whenComplete((produceResult, produceError) -> {
                          if (produceError != null) {
                            responseObserver.onError(produceError);
                          }
                        });
                  }
                } else {
                  responseObserver.onError(error);
                }
              });
        }
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
  public void consume(ConsumeRequest request, StreamObserver<LogRecord> responseObserver) {
    if (executor.isValidRequest(request, LogRecord::getDefaultInstance, responseObserver)) {
      Consumer<Record<byte[]>> consumer = record -> {
        responseObserver.onNext(LogRecord.newBuilder()
            .setOffset(record.offset())
            .setTimestamp(record.timestamp())
            .setValue(ByteString.copyFrom(record.value()))
            .build());
      };
      executor.getPrimitive(request.getId()).whenComplete((log, error) -> {
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
  }
}
