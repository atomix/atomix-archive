package io.atomix.primitive;

import java.util.concurrent.CompletableFuture;

/**
 * Managed asynchronous primitive.
 */
public interface ManagedAsyncPrimitive<P extends AsyncPrimitive> extends AsyncPrimitive {

  /**
   * Connects the primitive.
   *
   * @return a future to be completed once the primitive has been connected
   */
  CompletableFuture<P> connect();

}
