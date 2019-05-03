package io.atomix.cluster.impl;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.UnicastService;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;

/**
 * Cluster manager.
 */
@Component(ClusterConfig.class)
public class ClusterManager implements ClusterService, Managed<ClusterConfig> {
  @Dependency
  private UnicastService unicastService;
  @Dependency
  private BroadcastService broadcastService;
  @Dependency
  private MessagingService messagingService;
  @Dependency
  private ClusterMembershipService membershipService;
  @Dependency
  private ClusterCommunicationService communicationService;
  @Dependency
  private ClusterStreamingService streamingService;
  @Dependency
  private ClusterEventService eventService;

  @Override
  public UnicastService getUnicastService() {
    return unicastService;
  }

  @Override
  public BroadcastService getBroadcastService() {
    return broadcastService;
  }

  @Override
  public MessagingService getMessagingService() {
    return messagingService;
  }

  @Override
  public ClusterMembershipService getMembershipService() {
    return membershipService;
  }

  @Override
  public ClusterCommunicationService getCommunicationService() {
    return communicationService;
  }

  @Override
  public ClusterStreamingService getStreamingService() {
    return streamingService;
  }

  @Override
  public ClusterEventService getEventService() {
    return eventService;
  }
}
