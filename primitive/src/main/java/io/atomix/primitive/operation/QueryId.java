package io.atomix.primitive.operation;

/**
 * Query ID.
 */
public class QueryId<T, U> extends OperationId<T, U> {
  public QueryId(String id) {
    super(id);
  }

  @Override
  public OperationType type() {
    return OperationType.QUERY;
  }
}
