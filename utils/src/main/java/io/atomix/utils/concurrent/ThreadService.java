package io.atomix.utils.concurrent;

/**
 * Thread service.
 */
public interface ThreadService {

  /**
   * Returns the thread context factory.
   *
   * @return the thread context factory
   */
  ThreadContextFactory getFactory();

  /**
   * Creates a new thread context.
   *
   * @return a new thread context
   */
  ThreadContext createContext();

}
