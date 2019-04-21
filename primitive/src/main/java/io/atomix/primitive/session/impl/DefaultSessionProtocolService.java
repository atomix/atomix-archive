package io.atomix.primitive.session.impl;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionClientProtocol;
import io.atomix.primitive.session.SessionProtocolService;
import io.atomix.primitive.session.SessionServerProtocol;

/**
 * Default session protocol service.
 */
public class DefaultSessionProtocolService implements SessionProtocolService {
  private final ClusterCommunicationService communicationService;

  public DefaultSessionProtocolService(ClusterCommunicationService communicationService) {
    this.communicationService = communicationService;
  }

  @Override
  public SessionClientProtocol getClientProtocol(PartitionId partitionId) {
    return new SessionClientCommunicator(communicationService, new PrimitiveMessageContext(partitionId));
  }

  @Override
  public SessionServerProtocol getServerProtocol(PartitionId partitionId) {
    return new SessionServerCommunicator(communicationService, new PrimitiveMessageContext(partitionId));
  }
}
