package io.atomix.primitive.partition;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.log.LogClient;

/**
 * Log provider partition group.
 */
public interface LogProvider<P> {

  /**
   * Creates a new log topic.
   *
   * @param name the topic name
   * @param protocol the log protocol
   * @return a future to be completed with the log client
   */
  CompletableFuture<LogClient> createTopic(String name, P protocol);

}
