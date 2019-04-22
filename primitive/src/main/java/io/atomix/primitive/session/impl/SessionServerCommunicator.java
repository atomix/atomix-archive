package io.atomix.primitive.session.impl;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.session.SessionServerProtocol;

/**
 * Primitive server communicator.
 */
public class SessionServerCommunicator implements SessionServerProtocol {
  private final ClusterCommunicationService communicationService;
  private final PrimitiveMessageContext context;

  public SessionServerCommunicator(ClusterCommunicationService communicationService, PartitionId partitionId) {
    this.communicationService = communicationService;
    this.context = new PrimitiveMessageContext(partitionId);
  }

  @Override
  public void event(MemberId memberId, EventRequest request) {
    communicationService.unicast(context.eventSubject, request, EventRequest::toByteArray, memberId);
  }
}
