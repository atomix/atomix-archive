package io.atomix.primitive.partition;

import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.utils.event.AbstractEvent;

/**
 * Partition metadata event.
 */
public class PartitionMetadataEvent<T> extends AbstractEvent<PartitionMetadataEvent.Type, T> {

  /**
   * Metadata event type.
   */
  public enum Type {
    CREATE,
    UPDATE,
    DELETE,
  }

  private final String partitionGroup;

  public PartitionMetadataEvent(Type type, String partitionGroup, T subject) {
    super(type, subject);
    this.partitionGroup = partitionGroup;
  }

  /**
   * Returns the partition group.
   *
   * @return the partition group
   */
  public String partitionGroup() {
    return partitionGroup;
  }

  /**
   * Returns the partition metadata.
   *
   * @return the partition metadata
   */
  public T metadata() {
    return subject();
  }

  /**
   * Decodes the event.
   *
   * @param decoder the metadata decoder
   * @param <T>     the metadata type
   * @return the decoded event
   */
  public <T> PartitionMetadataEvent<T> decode(ByteArrayDecoder<T> decoder) {
    return new PartitionMetadataEvent<>(type(), partitionGroup(), ByteArrayDecoder.decode((byte[]) metadata(), decoder));
  }
}
