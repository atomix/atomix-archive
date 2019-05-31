package io.atomix.server.management;

/**
 * Node info.
 */
public class Node {
  private final String id;
  private final String host;
  private final int port;

  public Node(String id, String host, int port) {
    this.id = id;
    this.host = host;
    this.port = port;
  }

  /**
   * Returns the node ID.
   *
   * @return the node ID
   */
  public String id() {
    return id;
  }

  /**
   * Returns the node host.
   *
   * @return the node host
   */
  public String host() {
    return host;
  }

  /**
   * Returns the node port.
   *
   * @return the node port
   */
  public int port() {
    return port;
  }
}
