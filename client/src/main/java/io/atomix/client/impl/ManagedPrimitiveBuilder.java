package io.atomix.client.impl;

import java.time.Duration;

import io.atomix.api.headers.Name;
import io.atomix.client.PrimitiveBuilder;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.SyncPrimitive;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primitive builder for managed primitives.
 */
public abstract class ManagedPrimitiveBuilder<B extends PrimitiveBuilder<B, P>, P extends SyncPrimitive> extends PrimitiveBuilder<B, P> {
  protected Duration sessionTimeout = Duration.ofSeconds(30);

  protected ManagedPrimitiveBuilder(Name name, PrimitiveManagementService managementService) {
    super(name, managementService);
  }

  /**
   * Sets the session timeout.
   *
   * @param timeout the session timeout
   * @return the primitive builder
   */
  @SuppressWarnings("unchecked")
  public B withSessionTimeout(Duration timeout) {
    this.sessionTimeout = checkNotNull(timeout, "timeout cannot be null");
    return (B) this;
  }
}
