package io.atomix.core.test.protocol;

import java.util.concurrent.atomic.AtomicLong;

import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test state machine context.
 */
public class TestStateMachineContext implements StateMachine.Context {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestStateMachineContext.class);
  private final AtomicLong index = new AtomicLong();
  private final AtomicLong timestamp = new AtomicLong();
  private OperationType operationType;

  @Override
  public long getIndex() {
    return index.get();
  }

  @Override
  public long getTimestamp() {
    return timestamp.get();
  }

  @Override
  public OperationType getOperationType() {
    return operationType;
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  Command<byte[]> command(byte[] value) {
    operationType = OperationType.COMMAND;
    return new Command<>(index.incrementAndGet(), timestamp.accumulateAndGet(System.currentTimeMillis(), Math::max), value);
  }

  Query<byte[]> query(byte[] value) {
    operationType = OperationType.QUERY;
    return new Query<>(getIndex(), getTimestamp(), value);
  }
}
