package io.atomix.server.management;

import io.grpc.BindableService;

/**
 * Service registry.
 */
public interface ServiceRegistry {

  /**
   * Registers the given service.
   *
   * @param service the service to register
   */
  void register(BindableService service);

}
