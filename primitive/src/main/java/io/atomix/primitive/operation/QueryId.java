package io.atomix.primitive.operation;

/**
 * Query ID.
 */
public class QueryId<T, U> implements OperationId<T, U> {
  private final String id;

  public QueryId(String id) {
    this.id = id;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public OperationType type() {
    return OperationType.QUERY;
  }
}
