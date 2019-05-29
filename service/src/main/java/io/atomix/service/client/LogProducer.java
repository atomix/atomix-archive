package io.atomix.service.client;

import java.util.concurrent.CompletableFuture;

/**
 * Log producer.
 */
public interface LogProducer {

  /**
   * Appends a value to the log.
   *
   * @param value the value to append
   * @return a future to be completed once the value has been appended
   */
  CompletableFuture<Long> append(byte[] value);

}
