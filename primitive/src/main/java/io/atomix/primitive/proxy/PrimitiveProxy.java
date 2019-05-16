package io.atomix.primitive.proxy;

import java.util.concurrent.CompletableFuture;

/**
 * Primitive proxy.
 */
public interface PrimitiveProxy {

  /**
   * Returns the primitive name.
   *
   * @return the primitive name
   */
  String name();

  /**
   * Returns the service type.
   *
   * @return the service type
   */
  String type();

  /**
   * Deletes the primitive.
   *
   * @return a future to be completed once the primitive has been deleted
   */
  CompletableFuture<Void> delete();

}
