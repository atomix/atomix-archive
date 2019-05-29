package io.atomix.server.impl;

import java.util.function.BiFunction;

import io.atomix.api.primitive.PrimitiveId;
import io.atomix.service.PrimitiveService;
import io.atomix.server.protocol.ProtocolClient;
import io.atomix.service.protocol.ServiceId;
import io.atomix.service.proxy.ServiceProxy;

/**
 * Primitive executor.
 */
public class PrimitiveFactory<P extends ServiceProxy> {
  private final ProtocolClient client;
  private final PrimitiveService.Type serviceType;
  private final BiFunction<ServiceId, ProtocolClient, P> primitiveFactory;

  public PrimitiveFactory(
      ProtocolClient client,
      PrimitiveService.Type serviceType,
      BiFunction<ServiceId, ProtocolClient, P> primitiveFactory) {
    this.client = client;
    this.serviceType = serviceType;
    this.primitiveFactory = primitiveFactory;
  }

  /**
   * Returns the primitive with the given ID.
   *
   * @param id the primitive ID
   * @return the primitive
   */
  public P getPrimitive(PrimitiveId id) {
    return primitiveFactory.apply(getServiceId(id), client);
  }

  /**
   * Returns the service ID for the given primitive ID.
   *
   * @param id the primitive ID
   * @return the service ID
   */
  private ServiceId getServiceId(PrimitiveId id) {
    return ServiceId.newBuilder()
        .setName(id.getName())
        .setType(serviceType.name())
        .build();
  }
}
