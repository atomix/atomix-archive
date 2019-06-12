/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.protocols.raft;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.protobuf.util.JsonFormat;
import io.atomix.protocols.raft.protocol.RaftProtocolConfig;
import io.atomix.protocols.raft.protocol.RaftServiceGrpc;
import io.atomix.protocols.raft.protocol.impl.GrpcClientProtocol;
import io.atomix.protocols.raft.protocol.impl.GrpcServerProtocol;
import io.atomix.protocols.raft.storage.RaftStorage;
import io.atomix.server.management.Node;
import io.atomix.server.management.ProtocolManagementService;
import io.atomix.server.protocol.Protocol;
import io.atomix.server.protocol.ProtocolClient;
import io.atomix.server.protocol.ServiceProtocol;
import io.atomix.service.impl.ServiceManagerStateMachine;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.component.Component;

/**
 * Raft protocol implementation.
 */
public class RaftProtocol implements ServiceProtocol {
  public static final Type TYPE = new Type();

  @Component
  public static class Type implements Protocol.Type<RaftProtocolConfig> {
    private static final String NAME = "raft";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public RaftProtocolConfig parseConfig(InputStream is) throws IOException {
      RaftProtocolConfig.Builder builder = RaftProtocolConfig.newBuilder();
      JsonFormat.parser().ignoringUnknownFields().merge(new InputStreamReader(is), builder);
      return builder.build();
    }

    @Override
    public Protocol newProtocol(RaftProtocolConfig config, ProtocolManagementService managementService) {
      return new RaftProtocol(config, managementService);
    }
  }

  private final RaftProtocolConfig config;
  private final ProtocolManagementService managementService;
  private volatile RaftServer server;
  private volatile RaftClient client;

  private RaftProtocol(
      RaftProtocolConfig config,
      ProtocolManagementService managementService) {
    this.config = config;
    this.managementService = managementService;
  }

  @Override
  public CompletableFuture<Void> start() {
    return startServer().thenCompose(v -> startClient());
  }

  @Override
  public ProtocolClient getServiceClient() {
    return client;
  }

  private CompletableFuture<Void> startServer() {
    server = buildServer();
    return server.bootstrap(managementService.getCluster()
        .getNodes()
        .stream()
        .map(Node::id)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  private CompletableFuture<Void> startClient() {
    client = buildClient();
    return client.connect(managementService.getCluster()
        .getNodes()
        .stream()
        .map(Node::id)
        .collect(Collectors.toList()))
        .thenApply(v -> null);
  }

  private RaftServer buildServer() {
    Duration electionTimeout = Duration.ofSeconds(config.getElectionTimeout().getSeconds())
        .plusNanos(config.getElectionTimeout().getNanos());
    if (electionTimeout.isZero()) {
      electionTimeout = Duration.ofSeconds(5);
    }

    Duration heartbeatInterval = Duration.ofSeconds(config.getHeartbeatInterval().getSeconds())
        .plusNanos(config.getHeartbeatInterval().getNanos());
    if (heartbeatInterval.isZero()) {
      heartbeatInterval = Duration.ofMillis(250);
    }

    return RaftServer.builder(managementService.getCluster().getLocalNode().id())
        .withProtocol(new GrpcServerProtocol(
            managementService.getServiceProvider().getFactory(RaftServiceGrpc::newStub),
            managementService.getServiceRegistry()))
        .withStateMachine(new ServiceManagerStateMachine(managementService.getServiceTypeRegistry()))
        .withElectionTimeout(electionTimeout)
        .withHeartbeatInterval(heartbeatInterval)
        .withStorage(RaftStorage.builder()
            .withPrefix("raft")
            .withDirectory(!Strings.isNullOrEmpty(config.getStorage().getDirectory()) ? config.getStorage().getDirectory() : ".data")
            .withStorageLevel(StorageLevel.valueOf(config.getStorage().getLevel().name()))
            .withMaxSegmentSize(config.getStorage().getSegmentSize() > 0
                ? config.getStorage().getSegmentSize()
                : 1024 * 1024 * 32)
            .withMaxEntrySize(config.getStorage().getMaxEntrySize() > 0
                ? config.getStorage().getMaxEntrySize()
                : 1024 * 1024)
            .withFlushOnCommit(config.getStorage().getFlushOnCommit())
            .withDynamicCompaction(config.getCompaction().getDynamic())
            .withFreeDiskBuffer(config.getCompaction().getFreeDiskBuffer() > 0
                ? config.getCompaction().getFreeDiskBuffer()
                : .2)
            .withFreeMemoryBuffer(config.getCompaction().getFreeMemoryBuffer() > 0
                ? config.getCompaction().getFreeMemoryBuffer()
                : .2)
            .build())
        .withThreadContextFactory(managementService.getThreadService().getFactory())
        .build();
  }

  private RaftClient buildClient() {
    return RaftClient.builder()
        .withProtocol(new GrpcClientProtocol(
            managementService.getServiceProvider().getFactory(RaftServiceGrpc::newStub)))
        .withThreadContextFactory(managementService.getThreadService().getFactory())
        .build();
  }
}
