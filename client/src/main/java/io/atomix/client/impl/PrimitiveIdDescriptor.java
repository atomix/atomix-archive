package io.atomix.client.impl;

import com.google.protobuf.Message;
import io.atomix.api.protocol.DistributedLogProtocol;
import io.atomix.api.protocol.MultiPrimaryProtocol;
import io.atomix.api.protocol.MultiRaftProtocol;

/**
 * Primitive identifier descriptor.
 */
public interface PrimitiveIdDescriptor<I extends Message> {

  /**
   * Returns the name for the given ID.
   *
   * @param id the primitive ID
   * @return the primitive name
   */
  String getName(I id);

  /**
   * Returns a boolean indicating whether the given ID contains a multi-Raft protocol configuration.
   *
   * @param id the ID to check
   * @return indicates whether the given ID contains a multi-Raft protocol configuration
   */
  boolean hasMultiRaftProtocol(I id);

  /**
   * Returns the multi-Raft protocol configuration for the given ID.
   *
   * @param id the primitive ID
   * @return the multi-Raft protocol configuration for the given ID
   */
  MultiRaftProtocol getMultiRaftProtocol(I id);

  /**
   * Returns a boolean indicating whether the given ID contains a multi-primary protocol configuration.
   *
   * @param id the ID to check
   * @return indicates whether the given ID contains a multi-primary protocol configuration
   */
  boolean hasMultiPrimaryProtocol(I id);

  /**
   * Returns the multi-primary protocol configuration for the given ID.
   *
   * @param id the primitive ID
   * @return the multi-primary protocol configuration for the given ID
   */
  MultiPrimaryProtocol getMultiPrimaryProtocol(I id);

  /**
   * Returns a boolean indicating whether the given ID contains a distributed log protocol configuration.
   *
   * @param id the ID to check
   * @return indicates whether the given ID contains a distributed log protocol configuration
   */
  boolean hasDistributedLogProtocol(I id);

  /**
   * Returns the distributed log protocol configuration for the given ID.
   *
   * @param id the primitive ID
   * @return the distributed log protocol configuration for the given ID
   */
  DistributedLogProtocol getDistributedLogProtocol(I id);
}