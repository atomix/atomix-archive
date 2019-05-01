package io.atomix.primitive.config;

import java.time.Duration;

/**
 * Managed primitive configuration.
 */
public abstract class ManagedPrimitiveConfig<C extends ManagedPrimitiveConfig<C>> extends PrimitiveConfig<C> {
  private Duration sessionTimeout = Duration.ofSeconds(30);

  /**
   * Returns the primitive session timeout.
   *
   * @return the primitive session timeout
   */
  public Duration getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * Sets the primitive session timeout.
   *
   * @param sessionTimeout the primitive session timeout
   * @return the primitive configuration
   */
  @SuppressWarnings("unchecked")
  public C setSessionTimeout(Duration sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
    return (C) this;
  }
}
