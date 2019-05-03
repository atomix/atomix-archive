package io.atomix.utils.component;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test component.
 */
@Component(TestComponent1Config.class)
public class TestComponent1 implements TestComponentInterface1 {
  @Override
  public CompletableFuture<Void> start(TestComponent1Config config) {
    assertNotNull(config);
    assertEquals("1", config.getValue());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }
}
