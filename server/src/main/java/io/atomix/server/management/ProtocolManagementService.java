package io.atomix.server.management;

import io.atomix.server.protocol.ProtocolTypeRegistry;
import io.atomix.service.ServiceTypeRegistry;
import io.atomix.utils.concurrent.ThreadService;

/**
 * Protocol management service.
 */
public interface ProtocolManagementService {

  /**
   * Returns the local node
   *
   * @return the local node
   */
  Node getNode();

  /**
   * Returns the service registry.
   *
   * @return the service registry
   */
  ServiceRegistry getServiceRegistry();

  /**
   * Returns the service provider.
   *
   * @return the service provider
   */
  ServiceProvider getServiceProvider();

  /**
   * Returns the thread service.
   *
   * @return the thread service
   */
  ThreadService getThreadService();

  /**
   * Returns the protocol type registry.
   *
   * @return the protocol type registry
   */
  ProtocolTypeRegistry getProtocolTypeRegistry();

  /**
   * Returns the service type registry.
   *
   * @return the service type registry
   */
  ServiceTypeRegistry getServiceTypeRegistry();

  /**
   * Returns the primary election service.
   *
   * @return the primary election service
   */
  PrimaryElectionService getPrimaryElectionService();

}
