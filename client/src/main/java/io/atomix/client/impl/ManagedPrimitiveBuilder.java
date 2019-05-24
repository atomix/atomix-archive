package io.atomix.client.impl;

import java.time.Duration;

import io.atomix.client.PrimitiveBuilder;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.SyncPrimitive;
import io.atomix.client.PrimitiveType;
import io.atomix.client.ManagedPrimitiveConfig;

/**
 * Primitive builder for managed primitives.
 */
public abstract class ManagedPrimitiveBuilder<B extends PrimitiveBuilder<B, C, P>, C extends ManagedPrimitiveConfig, P extends SyncPrimitive> extends PrimitiveBuilder<B, C, P> {
  public ManagedPrimitiveBuilder(PrimitiveType type, String name, C config, PrimitiveManagementService managementService) {
    super(type, name, config, managementService);
  }

  /**
   * Sets the session timeout.
   *
   * @param timeout the session timeout
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withSessionTimeout(Duration timeout) {
    config.setSessionTimeout(timeout);
    return (B) this;
  }
}
