package io.atomix.protocols.log.partition.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.log.DistributedLogConsumer;
import io.atomix.primitive.log.LogConsumer;
import io.atomix.primitive.log.LogRecord;

/**
 * Log partition consumer.
 */
public class LogPartitionConsumer implements LogConsumer {
  private final DistributedLogConsumer consumer;

  public LogPartitionConsumer(DistributedLogConsumer consumer) {
    this.consumer = consumer;
  }

  @Override
  public CompletableFuture<Void> consume(long index, Consumer<LogRecord> consumer) {
    return this.consumer.consume(index, record -> consumer.accept(LogRecord.newBuilder()
        .setIndex(record.getIndex())
        .setTimestamp(record.getTimestamp())
        .setValue(record.getValue())
        .build()));
  }
}
