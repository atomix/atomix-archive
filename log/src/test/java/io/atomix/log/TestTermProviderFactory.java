package io.atomix.log;

/**
 * Test term provider factory.
 */
public class TestTermProviderFactory {
  private final TestTermProvider.Context context = new TestTermProvider.Context();

  public TermProvider newTermProvider(String memberId) {
    return new TestTermProvider(memberId, context);
  }
}
