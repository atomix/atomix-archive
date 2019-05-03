package io.atomix.cluster.impl;

import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.UnicastService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;

/**
 * Default bootstrap service.
 */
@Component
public class DefaultBootstrapService implements BootstrapService {
  @Dependency
  private MessagingService messagingService;

  @Dependency
  private UnicastService unicastService;

  @Dependency
  private BroadcastService broadcastService;

  @Override
  public MessagingService getMessagingService() {
    return messagingService;
  }

  @Override
  public UnicastService getUnicastService() {
    return unicastService;
  }

  @Override
  public BroadcastService getBroadcastService() {
    return broadcastService;
  }
}
