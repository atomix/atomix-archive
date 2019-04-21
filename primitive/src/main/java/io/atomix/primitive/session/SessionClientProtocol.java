package io.atomix.primitive.session;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

import io.atomix.primitive.service.impl.ListenRequest;
import io.atomix.primitive.session.impl.EventRequest;

/**
 * Primitive client protocol.
 */
public interface SessionClientProtocol {

  /**
   * Registers an event consumer.
   *
   * @param request  the listen request
   * @param consumer the event consumer to register
   * @param executor the event executor
   */
  void registerEventConsumer(ListenRequest request, Consumer<EventRequest> consumer, Executor executor);

  /**
   * Unregisters the event consumer
   *
   * @param request the listen request
   */
  void unregisterEventConsumer(ListenRequest request);

}
