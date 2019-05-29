package io.atomix.service.proxy;

/**
 * Abstract primitive proxy.
 */
public abstract class AbstractServiceProxy<T> implements ServiceProxy {
  private final T client;

  protected AbstractServiceProxy(T client) {
    this.client = client;
  }

  /**
   * Returns the primitive client.
   *
   * @return the primitive client
   */
  protected T getClient() {
    return client;
  }
}
