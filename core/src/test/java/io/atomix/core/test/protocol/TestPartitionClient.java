package io.atomix.core.test.protocol;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.StateMachine;
import io.atomix.utils.stream.StreamHandler;

/**
 * Test partition client.
 */
public class TestPartitionClient implements PartitionClient {
  private final StateMachine stateMachine;
  private final TestStateMachineContext context;

  public TestPartitionClient(StateMachine stateMachine, TestStateMachineContext context) {
    this.stateMachine = stateMachine;
    this.context = context;
  }

  @Override
  public CompletableFuture<byte[]> command(byte[] value) {
    return stateMachine.apply(context.command(value));
  }

  @Override
  public CompletableFuture<Void> command(byte[] value, StreamHandler<byte[]> handler) {
    return stateMachine.apply(context.command(value), handler);
  }

  @Override
  public CompletableFuture<byte[]> query(byte[] value) {
    return stateMachine.apply(context.query(value));
  }

  @Override
  public CompletableFuture<Void> query(byte[] value, StreamHandler<byte[]> handler) {
    return stateMachine.apply(context.query(value), handler);
  }
}
