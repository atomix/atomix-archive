package io.atomix.primitive.operation;

/**
 * Command ID.
 */
public class CommandId<T, U> extends OperationId<T, U> {
  public CommandId(String id) {
    super(id);
  }

  @Override
  public OperationType type() {
    return OperationType.COMMAND;
  }
}
