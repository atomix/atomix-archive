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
package io.atomix.core.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import com.google.common.io.BaseEncoding;
import io.atomix.grpc.log.ConsumeRequest;
import io.atomix.grpc.log.LogId;
import io.atomix.grpc.log.LogRecord;
import io.atomix.grpc.log.LogServiceGrpc;
import io.atomix.grpc.log.ProduceRequest;
import io.atomix.grpc.log.ProduceResponse;
import io.atomix.primitive.log.LogClient;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Log service implementation.
 */
public class LogServiceImpl extends LogServiceGrpc.LogServiceImplBase {
  private final PartitionService partitionService;

  public LogServiceImpl(PartitionService partitionService) {
    this.partitionService = partitionService;
  }

  /**
   * Returns a new client for the given log.
   *
   * @param id the log ID
   * @return the log client
   */
  private CompletableFuture<LogClient> getClient(LogId id) {
    return DistributedLogProtocol.builder(id.getLog().getGroup())
        .withNumPartitions(id.getLog().getPartitions())
        .build()
        .createTopic(id.getName(), partitionService);
  }

  @Override
  public StreamObserver<ProduceRequest> produce(StreamObserver<ProduceResponse> responseObserver) {
    Map<LogId, CompletableFuture<LogClient>> clients = new ConcurrentHashMap<>();
    return new StreamObserver<ProduceRequest>() {
      @Override
      public void onNext(ProduceRequest request) {
        clients.computeIfAbsent(request.getId(), LogServiceImpl.this::getClient)
            .whenComplete((client, clientError) -> {
              if (clientError == null) {
                if (request.getPartition() != 0) {
                  client.getPartition(request.getPartition())
                      .producer()
                      .append(request.getValue().toByteArray())
                      .whenComplete((response, error) -> {
                        if (error != null) {
                          responseObserver.onError(error);
                        }
                      });
                } else {
                  byte[] bytes = request.getValue().toByteArray();
                  client.getPartition(BaseEncoding.base16().encode(bytes))
                      .producer()
                      .append(bytes)
                      .whenComplete((response, error) -> {
                        if (error != null) {
                          responseObserver.onError(error);
                        }
                      });
                }
              } else {
                responseObserver.onError(clientError);
              }
            });
      }

      @Override
      public void onError(Throwable error) {
        responseObserver.onError(error);
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void consume(ConsumeRequest request, StreamObserver<LogRecord> responseObserver) {
    Consumer<io.atomix.primitive.log.LogRecord> consumer = record -> {
      responseObserver.onNext(LogRecord.newBuilder()
          .setOffset(record.getIndex())
          .setTimestamp(record.getTimestamp())
          .setValue(record.getValue())
          .build());
    };

    getClient(request.getId())
        .whenComplete((client, error) -> {
          if (error == null) {
            if (request.getPartition() != 0) {
              client.getPartition(request.getPartition())
                  .consumer()
                  .consume(consumer);
            } else {
              client.getPartitions()
                  .forEach(partition -> partition.consumer().consume(consumer));
            }
          } else {
            responseObserver.onError(error);
          }
        });
  }
}
