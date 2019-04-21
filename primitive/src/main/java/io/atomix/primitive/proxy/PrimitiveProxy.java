package io.atomix.primitive.proxy;

import io.atomix.utils.concurrent.ThreadContext;

/**
 * Primitive proxy.
 */
public interface PrimitiveProxy {

  /**
   * Returns the proxy thread context.
   *
   * @return the proxy thread context
   */
  ThreadContext context();

}
