package io.atomix.primitive;

import io.atomix.primitive.config.ManagedPrimitiveConfig;

/**
 * Primitive builder for managed primitives.
 */
public abstract class ManagedPrimitiveBuilder<B extends PrimitiveBuilder<B, C, P>, C extends ManagedPrimitiveConfig, P extends SyncPrimitive> extends PrimitiveBuilder<B, C, P> {
  public ManagedPrimitiveBuilder(PrimitiveType type, String name, C config, PrimitiveManagementService managementService) {
    super(type, name, config, managementService);
  }
}
