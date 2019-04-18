package io.atomix.core.test.protocol;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.StateMachine;

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
  public CompletableFuture<byte[]> write(byte[] value) {
    return stateMachine.apply(context.command(value));
  }

  @Override
  public CompletableFuture<byte[]> read(byte[] value) {
    return stateMachine.apply(context.query(value));
  }
}
