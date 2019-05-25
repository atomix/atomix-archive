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
package io.atomix.client.log.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import io.atomix.api.log.ConsumeRequest;
import io.atomix.api.log.LogRecord;
import io.atomix.api.log.LogServiceGrpc;
import io.atomix.api.log.ProduceRequest;
import io.atomix.api.log.ProduceResponse;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.log.AsyncDistributedLogPartition;
import io.atomix.client.log.DistributedLogPartition;
import io.atomix.client.log.Record;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;
import io.grpc.stub.StreamObserver;

/**
 * Default asynchronous distributed log partition implementation.
 */
public class DefaultAsyncDistributedLogPartition<E> implements AsyncDistributedLogPartition<E> {
  private final PrimitiveId id;
  private final int partitionId;
  private final LogServiceGrpc.LogServiceStub log;
  private final Serializer serializer;
  private volatile StreamObserver<ProduceRequest> producer;

  public DefaultAsyncDistributedLogPartition(PrimitiveId id, int partitionId, LogServiceGrpc.LogServiceStub log, Serializer serializer) {
    this.id = id;
    this.partitionId = partitionId;
    this.log = log;
    this.serializer = serializer;
  }

  @Override
  public int id() {
    return partitionId;
  }

  @Override
  public String name() {
    return id.getName();
  }

  /**
   * Encodes the given object using the configured {@link #serializer}.
   *
   * @param object the object to encode
   * @param <T>    the object type
   * @return the encoded bytes
   */
  private <T> byte[] encode(T object) {
    return object != null ? serializer.encode(object) : null;
  }

  /**
   * Decodes the given object using the configured {@link #serializer}.
   *
   * @param bytes the bytes to decode
   * @param <T>   the object type
   * @return the decoded object
   */
  private <T> T decode(byte[] bytes) {
    return bytes != null ? serializer.decode(bytes) : null;
  }

  /**
   * Produces the given bytes to the partition.
   *
   * @param bytes the bytes to produce
   * @return a future to be completed once the bytes have been written to the partition
   */
  CompletableFuture<Void> produce(byte[] bytes) {
    if (producer == null) {
      synchronized (this) {
        if (producer == null) {
          producer = log.produce(new StreamObserver<ProduceResponse>() {
            @Override
            public void onNext(ProduceResponse value) {

            }

            @Override
            public void onError(Throwable t) {
              onCompleted();
            }

            @Override
            public void onCompleted() {
              synchronized (DefaultAsyncDistributedLogPartition.this) {
                producer = null;
              }
            }
          });
        }
      }
    }
    producer.onNext(ProduceRequest.newBuilder()
        .setId(id)
        .setPartition(partitionId)
        .setValue(ByteString.copyFrom(bytes))
        .build());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> produce(E entry) {
    return produce(encode(entry));
  }

  @Override
  public CompletableFuture<Void> consume(long offset, Consumer<Record<E>> consumer) {
    log.consume(ConsumeRequest.newBuilder()
            .setId(id)
            .setPartition(partitionId)
            .setOffset(offset)
            .build(),
        new StreamObserver<LogRecord>() {
          @Override
          public void onNext(LogRecord record) {
            consumer.accept(new Record<>(record.getOffset(), record.getTimestamp(), decode(record.getValue().toByteArray())));
          }

          @Override
          public void onError(Throwable t) {

          }

          @Override
          public void onCompleted() {

          }
        });
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException("Cannot delete a single log partition"));
  }

  @Override
  public DistributedLogPartition<E> sync(Duration operationTimeout) {
    return new BlockingDistributedLogPartition<>(this, operationTimeout.toMillis());
  }
}
