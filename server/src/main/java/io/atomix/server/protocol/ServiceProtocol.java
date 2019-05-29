package io.atomix.server.protocol;

/**
 * Service protocol.
 */
public interface ServiceProtocol extends Protocol {

  /**
   * Returns the service client.
   *
   * @return the service client
   */
  ProtocolClient getServiceClient();

}
