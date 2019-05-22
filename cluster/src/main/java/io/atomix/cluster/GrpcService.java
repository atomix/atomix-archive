package io.atomix.cluster;

import java.util.function.Function;

import io.grpc.BindableService;
import io.grpc.Channel;

/**
 * Service registry.
 */
public interface GrpcService {

  /**
   * Registers the given service.
   *
   * @param service the service to register
   */
  void register(BindableService service);

  /**
   * Gets a remote service for the given node.
   *
   * @param host    the node host
   * @param port    the node port
   * @param factory the service factory
   * @param <T>     the service type
   * @return the service
   */
  <T> T getService(String host, int port, Function<Channel, T> factory);

}
