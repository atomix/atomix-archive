package io.atomix.protocols.raft;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Strings;
import com.google.protobuf.Descriptors;
import io.atomix.protocols.raft.protocol.RaftProtocolConfig;
import io.atomix.protocols.raft.protocol.RaftServiceGrpc;
import io.atomix.protocols.raft.protocol.impl.GrpcClientProtocol;
import io.atomix.protocols.raft.protocol.impl.GrpcServerProtocol;
import io.atomix.protocols.raft.storage.RaftStorage;
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
    public Class<RaftProtocolConfig> getConfigClass() {
      return RaftProtocolConfig.class;
    }

    @Override
    public Descriptors.Descriptor getConfigDescriptor() {
      return RaftProtocolConfig.getDescriptor();
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
    return server.bootstrap(config.getMembersList()).thenApply(v -> null);
  }

  private CompletableFuture<Void> startClient() {
    client = buildClient();
    return client.connect(config.getMembersList()).thenApply(v -> null);
  }

  private RaftServer buildServer() {
    Duration electionTimeout = Duration.ofSeconds(config.getElectionTimeout().getSeconds())
        .plusNanos(config.getElectionTimeout().getNanos());
    Duration heartbeatInterval = Duration.ofSeconds(config.getHeartbeatInterval().getSeconds())
        .plusNanos(config.getHeartbeatInterval().getNanos());
    return RaftServer.builder(managementService.getNode().id())
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
            .withMaxSegmentSize((int) (config.getStorage().getSegmentSize().getSize() > 0
                ? config.getStorage().getSegmentSize().getSize()
                : 1024 * 1024 * 32))
            .withMaxEntrySize((int) (config.getStorage().getMaxEntrySize().getSize() > 0
                ? config.getStorage().getMaxEntrySize().getSize()
                : 1024 * 1024))
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
        .withProtocol(new GrpcClientProtocol(managementService.getServiceProvider().getFactory(RaftServiceGrpc::newStub)))
        .withThreadContextFactory(managementService.getThreadService().getFactory())
        .build();
  }
}
