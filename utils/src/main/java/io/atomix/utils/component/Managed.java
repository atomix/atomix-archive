package io.atomix.utils.component;

import java.util.concurrent.CompletableFuture;

/**
 * Managed component.
 */
public interface Managed<C> {

  /**
   * Starts the managed component.
   *
   * @return a future to be completed once the component has been started
   */
  default CompletableFuture<Void> start() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Starts the managed component.
   *
   * @param config the component configuration
   * @return a future to be completed once the component has been started
   */
  default CompletableFuture<Void> start(C config) {
    return start();
  }

  /**
   * Stops the managed component.
   *
   * @return a future to be completed once the component has been stopped
   */
  default CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }

}
