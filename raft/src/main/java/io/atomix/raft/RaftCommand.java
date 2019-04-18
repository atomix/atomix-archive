package io.atomix.raft;

/**
 * Raft command.
 */
public class RaftCommand extends RaftOperation {
  public RaftCommand(long index, long timestamp, byte[] value) {
    super(index, timestamp, value);
  }

  @Override
  public Type type() {
    return Type.COMMAND;
  }
}
