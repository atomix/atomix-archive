package io.atomix.protocols.log;

import java.util.concurrent.CompletableFuture;

import com.google.common.base.Strings;
import com.google.protobuf.Descriptors;
import io.atomix.protocols.log.impl.PrimaryElectionTermProvider;
import io.atomix.protocols.log.protocol.DistributedLogServiceGrpc;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.LogServerProtocol;
import io.atomix.protocols.log.protocol.impl.GrpcProtocol;
import io.atomix.server.management.ProtocolManagementService;
import io.atomix.server.protocol.LogProtocol;
import io.atomix.server.protocol.Protocol;
import io.atomix.service.client.LogClient;
import io.atomix.utils.component.Component;

/**
 * Distributed log protocol.
 */
public class DistributedLogProtocol implements LogProtocol {
  public static final Type TYPE = new Type();

  @Component
  public static class Type implements Protocol.Type<LogProtocolConfig> {
    private static final String NAME = "log";

    @Override
    public String name() {
      return NAME;
    }

    @Override
    public Class<LogProtocolConfig> getConfigClass() {
      return LogProtocolConfig.class;
    }

    @Override
    public Descriptors.Descriptor getConfigDescriptor() {
      return LogProtocolConfig.getDescriptor();
    }

    @Override
    public Protocol newProtocol(LogProtocolConfig config, ProtocolManagementService managementService) {
      return new DistributedLogProtocol(config, managementService);
    }
  }

  private final LogProtocolConfig config;
  private final ProtocolManagementService managementService;
  private volatile DistributedLogServer server;
  private volatile DistributedLogClient client;

  private DistributedLogProtocol(LogProtocolConfig config, ProtocolManagementService managementService) {
    this.config = config;
    this.managementService = managementService;
  }

  @Override
  public LogClient getLogClient() {
    return client;
  }

  @Override
  public CompletableFuture<Void> start() {
    GrpcProtocol protocol = new GrpcProtocol(
        managementService.getServiceProvider().getFactory(DistributedLogServiceGrpc::newStub),
        managementService.getServiceRegistry());
    return startServer(protocol).thenCompose(v -> startClient(protocol));
  }

  private CompletableFuture<Void> startServer(LogServerProtocol protocol) {
    server = buildServer(protocol);
    return server.start().thenApply(v -> null);
  }

  private CompletableFuture<Void> startClient(LogClientProtocol protocol) {
    client = buildClient(protocol);
    return client.connect().thenApply(v -> null);
  }

  private DistributedLogServer buildServer(LogServerProtocol protocol) {
    return DistributedLogServer.builder()
        .withProtocol(protocol)
        .withDirectory(!Strings.isNullOrEmpty(config.getStorage().getDirectory()) ? config.getStorage().getDirectory() : ".data")
        .withStorageLevel(StorageLevel.valueOf(config.getStorage().getLevel().name()))
        .withMaxSegmentSize((int) (config.getStorage().getSegmentSize().getSize() > 0
            ? config.getStorage().getSegmentSize().getSize()
            : 1024 * 1024 * 32))
        .withMaxEntrySize((int) (config.getStorage().getMaxEntrySize().getSize() > 0
            ? config.getStorage().getMaxEntrySize().getSize()
            : 1024 * 1024))
        .withFlushOnCommit(config.getStorage().getFlushOnCommit())
        .withThreadContextFactory(managementService.getThreadService().getFactory())
        .build();
  }

  private DistributedLogClient buildClient(LogClientProtocol protocol) {
    return DistributedLogClient.builder()
        .withProtocol(protocol)
        .withThreadContextFactory(managementService.getThreadService().getFactory())
        .withTermProvider(new PrimaryElectionTermProvider(
            managementService.getPrimaryElectionService().getElectionFor(managementService.getPartitionId()),
            config.getMember()))
        .build();
  }
}
