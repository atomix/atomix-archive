package io.atomix.raft;

import io.atomix.utils.misc.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Raft operation.
 */
public abstract class RaftOperation {

  /**
   * Raft operation type.
   */
  public enum Type {
    COMMAND,
    QUERY,
  }

  private final long index;
  private final long timestamp;
  private final byte[] value;

  public RaftOperation(long index, long timestamp, byte[] value) {
    this.index = index;
    this.timestamp = timestamp;
    this.value = value;
  }

  /**
   * Returns the operation type.
   *
   * @return the operation type
   */
  public abstract Type type();

  /**
   * Returns the operation index.
   *
   * @return the operation index
   */
  public long index() {
    return index;
  }

  /**
   * Returns the operation timestamp.
   *
   * @return the operation timestamp
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the operation value.
   *
   * @return the operation value
   */
  public byte[] value() {
    return value;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("index", index())
        .add("timestamp", timestamp())
        .add("value", ArraySizeHashPrinter.of(value))
        .toString();
  }
}
