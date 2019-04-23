package io.atomix.primitive.operation;

/**
 * Command ID.
 */
public class CommandId<T, U> implements OperationId<T, U> {
  private final String id;

  public CommandId(String id) {
    this.id = id;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public OperationType type() {
    return OperationType.COMMAND;
  }
}
