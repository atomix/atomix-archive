package io.atomix.primitive.partition;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.PrimitiveClient;

/**
 * Service provider partition group.
 */
public interface ServiceProvider<P> {

  /**
   * Creates a new service client.
   *
   * @param name the service name
   * @param protocol the service protocol
   * @return a future to be completed with the service client
   */
  CompletableFuture<PrimitiveClient> createService(String name, P protocol);

}
