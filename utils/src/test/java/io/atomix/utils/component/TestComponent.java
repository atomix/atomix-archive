package io.atomix.utils.component;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;

/**
 * Test component.
 */
@Component(TestComponentConfig.class)
public class TestComponent implements Managed<TestComponentConfig> {
  @Dependency
  private TestComponentInterface1 component1;
  @Dependency
  private TestComponentInterface2 component2;
  @Dependency
  private TestComponent3 component3;

  @Override
  public CompletableFuture<Void> start(TestComponentConfig config) {
    assertNotNull(component1);
    assertNotNull(component2);
    assertNotNull(component3);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }
}
