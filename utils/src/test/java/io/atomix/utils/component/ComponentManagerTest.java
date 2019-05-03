package io.atomix.utils.component;

import org.junit.Test;

/**
 * Component manager test.
 */
public class ComponentManagerTest {
  @Test
  public void testComponentManager() throws Exception {
    ComponentManager<TestComponentConfig, TestComponent> manager = new ComponentManager<>(TestComponent.class);
    manager.start(new TestComponentConfig()).join();
    manager.stop().join();
  }
}
