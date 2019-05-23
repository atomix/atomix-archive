package io.atomix.core.impl;

import java.time.Duration;

import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.ManagedPrimitiveConfig;

/**
 * Primitive builder for managed primitives.
 */
public abstract class ManagedPrimitiveBuilder<B extends PrimitiveBuilder<B, C, P>, C extends ManagedPrimitiveConfig, P extends SyncPrimitive> extends AbstractPrimitiveBuilder<B, C, P> {
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
