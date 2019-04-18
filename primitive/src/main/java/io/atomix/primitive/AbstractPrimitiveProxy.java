package io.atomix.primitive;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.atomix.primitive.session.SessionClient;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Base class for primitive proxies.
 */
public abstract class AbstractPrimitiveProxy implements PrimitiveProxy {
  private final SessionClient client;

  public AbstractPrimitiveProxy(SessionClient client) {
    this.client = client;
  }

  @Override
  public String name() {
    return client.name();
  }

  @Override
  public PrimitiveType type() {
    return client.type();
  }

  @Override
  public ThreadContext context() {
    return client.context();
  }

  /**
   * Returns the primitive client.
   *
   * @return the primitive client
   */
  protected SessionClient getClient() {
    return client;
  }

  @Override
  public CompletableFuture<Void> connect() {
    return client.connect().thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> close() {
    return client.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return client.delete();
  }

  @Override
  public PrimitiveState getState() {
    return client.getState();
  }

  @Override
  public void onStateChange(Consumer<PrimitiveState> listener) {
    client.addStateChangeListener(listener);
  }
}