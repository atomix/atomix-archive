package io.atomix.server.protocol;

import io.atomix.service.client.LogClient;

/**
 * Log protocol.
 */
public interface LogProtocol extends Protocol {

  /**
   * Returns the log client.
   *
   * @return the log client
   */
  LogClient getLogClient();

}
