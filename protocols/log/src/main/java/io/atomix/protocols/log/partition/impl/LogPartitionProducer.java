package io.atomix.protocols.log.partition.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.log.DistributedLogProducer;
import io.atomix.primitive.log.LogProducer;

/**
 * Log partition producer.
 */
public class LogPartitionProducer implements LogProducer {
  private final DistributedLogProducer producer;

  public LogPartitionProducer(DistributedLogProducer producer) {
    this.producer = producer;
  }

  @Override
  public CompletableFuture<Long> append(byte[] value) {
    return producer.append(value);
  }
}
