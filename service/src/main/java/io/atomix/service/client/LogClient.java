package io.atomix.service.client;

/**
 * Log client.
 */
public interface LogClient {

  /**
   * Returns the log producer.
   *
   * @return the log producer
   */
  LogProducer producer();

  /**
   * Returns the log consumer.
   *
   * @return the log consumer
   */
  LogConsumer consumer();

}
