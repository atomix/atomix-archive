package io.atomix.cluster.messaging;

import io.atomix.utils.Managed;

/**
 * Managed streaming service.
 */
public interface ManagedClusterStreamingService extends ClusterStreamingService, Managed<ClusterStreamingService> {
}
