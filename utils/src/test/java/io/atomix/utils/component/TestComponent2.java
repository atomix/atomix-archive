package io.atomix.utils.component;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test component.
 */
@Component(TestComponent2Config.class)
public class TestComponent2 implements TestComponentInterface2, Managed<TestComponent2Config> {
  @Dependency
  private TestComponent3 component3;

  @Override
  public CompletableFuture<Void> start(TestComponent2Config config) {
    assertNotNull(component3);
    assertNotNull(config);
    assertEquals("2", config.getValue());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }
}
