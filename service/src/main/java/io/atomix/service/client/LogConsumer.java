package io.atomix.service.client;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.service.protocol.LogRecord;

/**
 * Log consumer.
 */
public interface LogConsumer {

  /**
   * Consumes records from the given offset.
   *
   * @param index the offset from which to consume records
   * @param consumer the record consumer
   * @return a future to be completed once consuming begins
   */
  CompletableFuture<Void> consume(long index, Consumer<LogRecord> consumer);

}
