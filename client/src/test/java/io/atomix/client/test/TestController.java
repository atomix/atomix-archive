package io.atomix.client.test;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.atomix.api.controller.ControllerServiceGrpc;
import io.atomix.api.controller.GetPartitionGroupsRequest;
import io.atomix.api.controller.GetPartitionGroupsResponse;
import io.atomix.api.partition.PartitionGroup;
import io.atomix.server.NodeConfig;
import io.atomix.server.ServerConfig;
import io.atomix.server.management.impl.ServiceRegistryImpl;
import io.atomix.utils.component.Managed;
import io.grpc.stub.StreamObserver;

/**
 * Test controller.
 */
public class TestController extends ControllerServiceGrpc.ControllerServiceImplBase implements Managed {
  private final ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
  private final int port;
  private final Collection<PartitionGroup> groups;

  public TestController(int port, Collection<PartitionGroup> groups) {
    this.port = port;
    this.groups = groups;
  }

  @Override
  public void getPartitionGroups(GetPartitionGroupsRequest request, StreamObserver<GetPartitionGroupsResponse> responseObserver) {
    responseObserver.onNext(GetPartitionGroupsResponse.newBuilder()
        .addAllGroups(groups)
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public CompletableFuture<Void> start() {
    return serviceRegistry.start(ServerConfig.newBuilder()
        .setNode(NodeConfig.newBuilder()
            .setPort(port)
            .build())
        .build());
  }
}
