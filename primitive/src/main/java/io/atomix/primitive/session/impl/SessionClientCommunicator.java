package io.atomix.primitive.session.impl;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.impl.ListenRequest;
import io.atomix.primitive.session.SessionClientProtocol;
import io.atomix.primitive.util.ByteArrayDecoder;

/**
 * Primitive client communicator.
 */
public class SessionClientCommunicator implements SessionClientProtocol {
  private final ClusterCommunicationService communicationService;
  private final PrimitiveMessageContext context;

  public SessionClientCommunicator(ClusterCommunicationService communicationService, PartitionId partitionId) {
    this.communicationService = communicationService;
    this.context = new PrimitiveMessageContext(partitionId);
  }

  @Override
  public void registerEventConsumer(ListenRequest request, Consumer<EventRequest> consumer, Executor executor) {
    communicationService.subscribe(
        context.eventSubject,
        bytes -> ByteArrayDecoder.decode(bytes, EventRequest::parseFrom),
        consumer,
        executor);
  }

  @Override
  public void unregisterEventConsumer(ListenRequest request) {
    communicationService.unsubscribe(context.eventSubject);
  }
}
