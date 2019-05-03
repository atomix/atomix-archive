/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.protocols.raft.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.raft.RaftClient;
import io.atomix.raft.RaftException;
import io.atomix.raft.protocol.RaftClientProtocol;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * StoragePartition client.
 */
public class RaftPartitionClient implements PartitionClient, Managed<RaftPartitionClient> {

  private final Logger log = getLogger(getClass());

  private final RaftPartition partition;
  private final RaftClientProtocol protocol;
  private final ThreadContextFactory threadContextFactory;
  private RaftClient client;

  public RaftPartitionClient(
      RaftPartition partition,
      RaftClientProtocol protocol,
      ThreadContextFactory threadContextFactory) {
    this.partition = partition;
    this.protocol = protocol;
    this.threadContextFactory = threadContextFactory;
  }

  /**
   * Returns the partition term.
   *
   * @return the partition term
   */
  public long term() {
    return client != null ? client.term() : 0;
  }

  /**
   * Returns the partition leader.
   *
   * @return the partition leader
   */
  public MemberId leader() {
    return client != null ? MemberId.from(client.leader()) : null;
  }

  @Override
  public CompletableFuture<byte[]> command(byte[] value) {
    return Futures.transformExceptions(client.write(value), this::convertException);
  }

  @Override
  public CompletableFuture<Void> command(byte[] value, StreamHandler<byte[]> handler) {
    return Futures.transformExceptions(client.write(value, handler), this::convertException);
  }

  @Override
  public CompletableFuture<byte[]> query(byte[] value) {
    return Futures.transformExceptions(client.read(value), this::convertException);
  }

  @Override
  public CompletableFuture<Void> query(byte[] value, StreamHandler<byte[]> handler) {
    return Futures.transformExceptions(client.read(value, handler), this::convertException);
  }

  private Throwable convertException(Throwable error) {
    if (error instanceof CompletionException) {
      error = error.getCause();
    }
    if (error instanceof RaftException.ApplicationException) {
      return new PrimitiveException.ServiceException(error.getMessage());
    } else if (error instanceof RaftException.CommandFailure) {
      return new PrimitiveException.CommandFailure(error.getMessage());
    } else if (error instanceof RaftException.QueryFailure) {
      return new PrimitiveException.QueryFailure(error.getMessage());
    } else if (error instanceof RaftException.UnknownSession) {
      return new PrimitiveException.UnknownSession(error.getMessage());
    } else if (error instanceof RaftException.UnknownService) {
      return new PrimitiveException.UnknownService(error.getMessage());
    } else {
      return error;
    }
  }

  @Override
  public CompletableFuture<RaftPartitionClient> start() {
    synchronized (RaftPartitionClient.this) {
      client = newRaftClient(protocol);
    }
    return client.connect(partition.members()
        .stream()
        .map(MemberId::id)
        .collect(Collectors.toList()))
        .whenComplete((r, e) -> {
          if (e == null) {
            log.debug("Successfully started client for partition {}", partition.id());
          } else {
            log.warn("Failed to start client for partition {}", partition.id(), e);
          }
        }).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return client != null ? client.close() : CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isRunning() {
    return client != null;
  }

  private RaftClient newRaftClient(RaftClientProtocol protocol) {
    return RaftClient.builder()
        .withClientId(partition.name())
        .withProtocol(protocol)
        .withThreadContextFactory(threadContextFactory)
        .build();
  }
}
