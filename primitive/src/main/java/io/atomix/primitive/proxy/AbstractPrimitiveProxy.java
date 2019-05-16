package io.atomix.primitive.proxy;

/**
 * Abstract primitive proxy.
 */
public abstract class AbstractPrimitiveProxy<T> implements PrimitiveProxy {
  private final T client;

  protected AbstractPrimitiveProxy(T client) {
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
