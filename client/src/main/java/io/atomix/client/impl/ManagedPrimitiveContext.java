package io.atomix.client.impl;

import java.time.Duration;

import io.atomix.client.PrimitiveType;

/**
 * Managed primitive context.
 */
public class ManagedPrimitiveContext {
  private final long sessionId;
  private final String name;
  private final PrimitiveType type;
  private final Duration timeout;

  public ManagedPrimitiveContext(long sessionId, String name, PrimitiveType type, Duration timeout) {
    this.sessionId = sessionId;
    this.name = name;
    this.type = type;
    this.timeout = timeout;
  }

  /**
   * Returns the primitive session ID.
   *
   * @return the primitive session ID
   */
  public long sessionId() {
    return sessionId;
  }

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the primitive type.
   *
   * @return the primitive type
   */
  public PrimitiveType type() {
    return type;
  }

  /**
   * Returns the session timeout.
   *
   * @return the session timeout
   */
  public Duration timeout() {
    return timeout;
  }
}