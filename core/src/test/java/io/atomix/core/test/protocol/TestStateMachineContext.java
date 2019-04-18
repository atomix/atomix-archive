package io.atomix.core.test.protocol;

import java.util.concurrent.atomic.AtomicLong;

import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.Role;
import io.atomix.primitive.service.StateMachine;

/**
 * Test state machine context.
 */
public class TestStateMachineContext implements StateMachine.Context {
  private final AtomicLong index = new AtomicLong();
  private final AtomicLong timestamp = new AtomicLong();

  @Override
  public long getIndex() {
    return index.get();
  }

  @Override
  public long getTimestamp() {
    return timestamp.get();
  }

  @Override
  public Role getRole() {
    return Role.PRIMARY;
  }

  Command<byte[]> command(byte[] value) {
    return new Command<>(index.incrementAndGet(), timestamp.accumulateAndGet(System.currentTimeMillis(), Math::max), value);
  }

  Query<byte[]> query(byte[] value) {
    return new Query<>(getIndex(), getTimestamp(), value);
  }
}
