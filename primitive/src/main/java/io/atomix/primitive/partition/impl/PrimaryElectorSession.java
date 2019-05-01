package io.atomix.primitive.partition.impl;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.impl.SessionEnabledAsyncPrimitive;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.utils.stream.StreamHandler;

/**
 * Primary elector session.
 */
public class PrimaryElectorSession
    extends SessionEnabledAsyncPrimitive<PrimaryElectorProxy, PrimaryElector>
    implements PrimaryElector {
  private final Set<Consumer<PrimaryElectionEvent>> eventListeners = new CopyOnWriteArraySet<>();

  public PrimaryElectorSession(
      PrimaryElectorProxy proxy,
      Duration timeout,
      PrimitiveManagementService managementService) {
    super(proxy, timeout, managementService);
  }

  @Override
  public PrimitiveType type() {
    return PrimaryElectorType.instance();
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(PartitionId partitionId, GroupMember member) {
    return execute(PrimaryElectorProxy::enter, EnterRequest.newBuilder()
        .setPartitionId(partitionId)
        .setMember(member)
        .build())
        .thenApply(response -> response.getTerm());
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm(PartitionId partitionId) {
    return execute(PrimaryElectorProxy::getTerm, GetTermRequest.newBuilder()
        .setPartitionId(partitionId)
        .build())
        .thenApply(response -> response.getTerm());
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(Consumer<PrimaryElectionEvent> listener) {
    if (eventListeners.isEmpty()) {
      return execute(PrimaryElectorProxy::stream, ListenRequest.newBuilder().build(), new StreamHandler<PrimaryElectionEvent>() {
        @Override
        public void next(PrimaryElectionEvent event) {
          eventListeners.forEach(listener -> listener.accept(event));
        }

        @Override
        public void complete() {

        }

        @Override
        public void error(Throwable error) {

        }
      }).thenApply(response -> null);
    } else {
      eventListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(Consumer<PrimaryElectionEvent> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public SyncPrimitive sync() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SyncPrimitive sync(Duration operationTimeout) {
    throw new UnsupportedOperationException();
  }
}
