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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.primitive.partition.Partition;
import io.atomix.primitive.service.StateMachine;
import io.atomix.protocols.raft.partition.RaftPartition;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;
import io.atomix.raft.RaftServer;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.storage.StorageException;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * {@link Partition} server.
 */
public class RaftPartitionServer {

  private final Logger log = getLogger(getClass());

  private static final long ELECTION_TIMEOUT_MILLIS = 2500;
  private static final long HEARTBEAT_INTERVAL_MILLIS = 250;

  private final MemberId localMemberId;
  private final RaftPartition partition;
  private final RaftPartitionGroupConfig config;
  private final ClusterCommunicationService clusterCommunicator;
  private final ClusterStreamingService streamingService;
  private final ThreadContextFactory threadContextFactory;
  private RaftServer server;

  public RaftPartitionServer(
      RaftPartition partition,
      RaftPartitionGroupConfig config,
      MemberId localMemberId,
      ClusterCommunicationService clusterCommunicator,
      ClusterStreamingService streamingService,
      ThreadContextFactory threadContextFactory) {
    this.partition = partition;
    this.config = config;
    this.localMemberId = localMemberId;
    this.clusterCommunicator = clusterCommunicator;
    this.streamingService = streamingService;
    this.threadContextFactory = threadContextFactory;
  }

  /**
   * Starts the partition server.
   *
   * @param stateMachine the state machine
   * @return a future to be completed once the server is started
   */
  public CompletableFuture<RaftPartitionServer> start(StateMachine stateMachine) {
    log.info("Starting server for partition {}", partition.id());
    CompletableFuture<RaftServer> serverOpenFuture;
    if (partition.members().contains(localMemberId)) {
      if (server != null && server.isRunning()) {
        return CompletableFuture.completedFuture(null);
      }
      synchronized (this) {
        try {
          server = buildServer(stateMachine);
        } catch (StorageException e) {
          return Futures.exceptionalFuture(e);
        }
      }
      serverOpenFuture = server.bootstrap(partition.members()
          .stream()
          .map(MemberId::id)
          .collect(Collectors.toList()));
    } else {
      serverOpenFuture = CompletableFuture.completedFuture(null);
    }
    return serverOpenFuture.whenComplete((r, e) -> {
      if (e == null) {
        log.debug("Successfully started server for partition {}", partition.id());
      } else {
        log.warn("Failed to start server for partition {}", partition.id(), e);
      }
    }).thenApply(v -> this);
  }

  /**
   * Stops the partition server.
   *
   * @return a future to be completed once the server is stopped
   */
  public CompletableFuture<Void> stop() {
    return server.shutdown();
  }

  /**
   * Closes the server and exits the partition.
   *
   * @return future that is completed when the operation is complete
   */
  public CompletableFuture<Void> leave() {
    return server.leave();
  }

  /**
   * Takes a snapshot of the partition server.
   *
   * @return a future to be completed once the snapshot has been taken
   */
  public CompletableFuture<Void> snapshot() {
    return server.compact();
  }

  /**
   * Deletes the server.
   */
  public void delete() {
    try {
      Files.walkFileTree(partition.dataDirectory().toPath(), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      log.error("Failed to delete partition: {}", e);
    }
  }

  private RaftServer buildServer(StateMachine stateMachine) {
    return RaftServer.builder(localMemberId.id())
        .withName(partition.name())
        .withProtocol(new RaftServerCommunicator(
            partition.name(),
            clusterCommunicator,
            streamingService))
        .withStateMachine(new RaftPartitionStateMachine(stateMachine))
        .withElectionTimeout(Duration.ofMillis(ELECTION_TIMEOUT_MILLIS))
        .withHeartbeatInterval(Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS))
        .withStorage(RaftStorage.builder()
            .withPrefix(partition.name())
            .withDirectory(partition.dataDirectory())
            .withStorageLevel(config.getStorageConfig().getLevel())
            .withMaxSegmentSize((int) config.getStorageConfig().getSegmentSize().bytes())
            .withMaxEntrySize((int) config.getStorageConfig().getMaxEntrySize().bytes())
            .withFlushOnCommit(config.getStorageConfig().isFlushOnCommit())
            .withDynamicCompaction(config.getCompactionConfig().isDynamic())
            .withFreeDiskBuffer(config.getCompactionConfig().getFreeDiskBuffer())
            .withFreeMemoryBuffer(config.getCompactionConfig().getFreeMemoryBuffer())
            .build())
        .withThreadContextFactory(threadContextFactory)
        .build();
  }
}
