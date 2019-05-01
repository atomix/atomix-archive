package io.atomix.primitive;

import java.time.Duration;

import io.atomix.primitive.config.ManagedPrimitiveConfig;

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
