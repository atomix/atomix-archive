package io.atomix.server.management;

import java.util.function.Function;

import io.grpc.Channel;

/**
 * gRPC service provider.
 */
public interface ServiceProvider {

  /**
   * Returns a new service factory for the given stub factory.
   *
   * @param factory the stub factory
   * @param <T> the service type
   * @return the service factory
   */
  <T> ServiceFactory<T> getFactory(Function<Channel, T> factory);

}
