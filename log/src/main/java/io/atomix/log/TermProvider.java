package io.atomix.log;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Term provider.
 */
public interface TermProvider {

  /**
   * Returns the current term.
   *
   * @return the current term
   */
  CompletableFuture<Term> getTerm();

  /**
   * Adds a term change listener.
   *
   * @param listener the term change listener
   * @return a future to be completed once the listener has been added
   */
  CompletableFuture<Void> addListener(Consumer<Term> listener);

  /**
   * Removes a term change listener.
   *
   * @param listener the term change listener
   * @return a future to be completed once the listener has been removed
   */
  CompletableFuture<Void> removeListener(Consumer<Term> listener);

  /**
   * Joins the term.
   *
   * @return a future to be completed once the term has been joined
   */
  CompletableFuture<Void> join();

  /**
   * Leaves the term.
   *
   * @return a future to be completed once the term has been left
   */
  CompletableFuture<Void> leave();

}
