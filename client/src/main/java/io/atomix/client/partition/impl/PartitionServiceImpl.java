package io.atomix.client.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.api.partition.GetPartitionGroupsRequest;
import io.atomix.api.partition.GetPartitionGroupsResponse;
import io.atomix.api.partition.PartitionServiceGrpc;
import io.atomix.client.channel.ChannelFactory;
import io.atomix.client.partition.PartitionGroup;
import io.atomix.client.partition.PartitionService;
import io.grpc.stub.StreamObserver;

/**
 * Partition service implementation.
 */
public class PartitionServiceImpl implements PartitionService {
  private final PartitionServiceGrpc.PartitionServiceStub service;

  public PartitionServiceImpl(ChannelFactory channelFactory) {
    this.service = PartitionServiceGrpc.newStub(channelFactory.getChannel());
  }

  @Override
  public CompletableFuture<PartitionGroup> getPartitionGroup(String name) {
    return this.<GetPartitionGroupsResponse>execute(observer ->
        service.getPartitionGroups(GetPartitionGroupsRequest.newBuilder().build(), observer))
        .thenApply(response -> {
          io.atomix.api.partition.PartitionGroup group = response.getGroupsList().stream()
              .filter(g -> g.getName().equals(name))
              .findFirst()
              .orElse(null);
          return new PartitionGroupImpl(group);
        });
  }

  private <T> CompletableFuture<T> execute(Consumer<StreamObserver<T>> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    callback.accept(new StreamObserver<T>() {
      @Override
      public void onNext(T value) {
        future.complete(value);
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {

      }
    });
    return future;
  }
}
