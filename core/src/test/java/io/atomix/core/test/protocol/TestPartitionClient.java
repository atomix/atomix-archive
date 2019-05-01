package io.atomix.core.test.protocol;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.partition.PartitionClient;
import io.atomix.utils.stream.StreamHandler;

/**
 * Test partition client.
 */
public class TestPartitionClient implements PartitionClient {
  private final TestStateMachine stateMachine;

  public TestPartitionClient(TestStateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  @Override
  public CompletableFuture<byte[]> command(byte[] value) {
    return stateMachine.apply(stateMachine.context().command(value));
  }

  @Override
  public CompletableFuture<Void> command(byte[] value, StreamHandler<byte[]> handler) {
    return stateMachine.apply(stateMachine.context().command(value), handler);
  }

  @Override
  public CompletableFuture<byte[]> query(byte[] value) {
    return stateMachine.apply(stateMachine.context().query(value));
  }

  @Override
  public CompletableFuture<Void> query(byte[] value, StreamHandler<byte[]> handler) {
    return stateMachine.apply(stateMachine.context().query(value), handler);
  }
}
