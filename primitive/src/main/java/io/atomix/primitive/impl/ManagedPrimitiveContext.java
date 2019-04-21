package io.atomix.primitive.impl;

import java.time.Duration;

import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.session.SessionId;

/**
 * Managed primitive context.
 */
public class ManagedPrimitiveContext {
  private final MemberId memberId;
  private final SessionId sessionId;
  private final String name;
  private final PrimitiveType type;
  private final Duration timeout;

  public ManagedPrimitiveContext(MemberId memberId, SessionId sessionId, String name, PrimitiveType type, Duration timeout) {
    this.memberId = memberId;
    this.sessionId = sessionId;
    this.name = name;
    this.type = type;
    this.timeout = timeout;
  }

  /**
   * Returns the local member ID.
   *
   * @return the local member ID
   */
  public MemberId memberId() {
    return memberId;
  }

  /**
   * Returns the primitive session ID.
   *
   * @return the primitive session ID
   */
  public SessionId sessionId() {
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
