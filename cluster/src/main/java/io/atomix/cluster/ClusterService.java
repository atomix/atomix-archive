package io.atomix.cluster;

import io.atomix.cluster.messaging.BroadcastService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.ClusterStreamingService;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.UnicastService;
import io.atomix.utils.net.Address;

/**
 * Cluster service.
 */
public interface ClusterService {

  /**
   * Returns the cluster unicast service.
   * <p>
   * The unicast service supports unreliable uni-directional messaging via UDP. This is a
   * low-level cluster communication API. For higher level messaging, use the
   * {@link #getCommunicationService() communication service} or {@link #getEventService() event service}.
   *
   * @return the cluster unicast service
   */
  UnicastService getUnicastService();

  /**
   * Returns the cluster broadcast service.
   * <p>
   * The broadcast service is used to broadcast messages to all nodes in the cluster via multicast.
   * The broadcast service is disabled by default. To enable broadcast, the cluster must be configured with
   * {@link AtomixClusterBuilder#withMulticastEnabled() multicast enabled}.
   *
   * @return the cluster broadcast service
   */
  BroadcastService getBroadcastService();

  /**
   * Returns the cluster messaging service.
   * <p>
   * The messaging service is used for direct point-to-point messaging between nodes by {@link Address}. This is a
   * low-level cluster communication API. For higher level messaging, use the
   * {@link #getCommunicationService() communication service} or {@link #getEventService() event service}.
   *
   * @return the cluster messaging service
   */
  MessagingService getMessagingService();

  /**
   * Returns the cluster membership service.
   * <p>
   * The membership service manages cluster membership information and failure detection.
   *
   * @return the cluster membership service
   */
  ClusterMembershipService getMembershipService();

  /**
   * Returns the cluster communication service.
   * <p>
   * The cluster communication service is used for high-level unicast, multicast, broadcast, and request-reply messaging.
   *
   * @return the cluster communication service
   */
  ClusterCommunicationService getCommunicationService();

  /**
   * Returns the cluster streaming service.
   * <p>
   * The cluster streaming service is used for high-level client, server, and bi-directional streams.
   *
   * @return the cluster streaming service
   */
  ClusterStreamingService getStreamingService();

  /**
   * Returns the cluster event service.
   * <p>
   * The cluster event service is used for high-level publish-subscribe messaging.
   *
   * @return the cluster event service
   */
  ClusterEventService getEventService();

}
