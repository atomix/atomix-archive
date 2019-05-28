package io.atomix.utils.component;

import org.junit.Test;

/**
 * Component manager test.
 */
public class ComponentManagerTest {
  @Test
  public void testComponentManager() throws Exception {
    ComponentManager<TestComponentConfig, TestComponent> manager = new ComponentManager<>(TestComponent.class);
    manager.start(TestComponentConfig.newBuilder().build()).join();
    manager.stop().join();
  }
}
