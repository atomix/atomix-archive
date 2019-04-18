package io.atomix.raft;

/**
 * Raft query.
 */
public class RaftQuery extends RaftOperation {
  public RaftQuery(long index, long timestamp, byte[] value) {
    super(index, timestamp, value);
  }

  @Override
  public Type type() {
    return Type.QUERY;
  }
}
