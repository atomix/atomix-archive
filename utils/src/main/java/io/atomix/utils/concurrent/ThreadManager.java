package io.atomix.utils.concurrent;

import java.util.concurrent.CompletableFuture;

import io.atomix.utils.component.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread manager.
 */
public class ThreadManager implements ThreadService, Managed {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadManager.class);

  private ThreadContextFactory threadContextFactory;

  @Override
  public ThreadContextFactory getFactory() {
    return threadContextFactory;
  }

  @Override
  public ThreadContext createContext() {
    return threadContextFactory.createContext();
  }

  @Override
  public CompletableFuture<Void> start() {
    threadContextFactory = new ThreadPoolContextFactory(
        "atomix-management-%d",
        Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 32), 8),
        LOGGER);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    threadContextFactory.close();
    return CompletableFuture.completedFuture(null);
  }
}
