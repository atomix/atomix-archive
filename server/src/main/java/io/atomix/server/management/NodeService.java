package io.atomix.server.management;

/**
 * Node service.
 */
public interface NodeService {

  /**
   * Returns the local node info.
   *
   * @return the local node info
   */
  Node getNode();

}
