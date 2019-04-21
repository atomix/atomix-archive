package io.atomix.primitive.session;

import io.atomix.primitive.partition.PartitionId;

/**
 * Primitive protocol service.
 */
public interface SessionProtocolService {

  /**
   * Returns the client protocol for the given partition.
   *
   * @param partitionId the partition ID
   * @return the client protocol for the given partition
   */
  SessionClientProtocol getClientProtocol(PartitionId partitionId);

  /**
   * Returns the server protocol for the given partition.
   *
   * @param partitionId the partition ID
   * @return the server protocol for the given partition
   */
  SessionServerProtocol getServerProtocol(PartitionId partitionId);

}
