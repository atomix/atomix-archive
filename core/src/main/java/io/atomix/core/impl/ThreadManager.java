package io.atomix.core.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.core.AtomixConfig;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.BlockingAwareThreadPoolContextFactory;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread manager.
 */
@Component(AtomixConfig.class)
public class ThreadManager implements ThreadService, Managed<AtomixConfig> {
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
  public CompletableFuture<Void> start(AtomixConfig config) {
    threadContextFactory = new BlockingAwareThreadPoolContextFactory(
        "atomix-" + config.getClusterConfig().getNodeConfig().getId().id() + "-primitives-%d",
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
