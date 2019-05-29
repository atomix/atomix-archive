package io.atomix.log;

import io.atomix.protocols.log.TermProvider;

/**
 * Test term provider factory.
 */
public class TestTermProviderFactory {
  private final TestTermProvider.Context context = new TestTermProvider.Context();

  public TermProvider newTermProvider(String memberId) {
    return new TestTermProvider(memberId, context);
  }
}
