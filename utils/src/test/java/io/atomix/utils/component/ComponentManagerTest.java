package io.atomix.utils.component;

import org.junit.Test;

/**
 * Component manager test.
 */
public class ComponentManagerTest {
  @Test
  public void testComponentManager() throws Exception {
    ComponentManager<TestComponentConfig, TestComponent> manager = new ComponentManager<>(TestComponent.class);
    manager.start(TestComponentConfig.newBuilder()
        .setTestComponent1Config(TestComponent1Config.newBuilder()
            .setValue("1")
            .build())
        .setTestComponent2Config(TestComponent2Config.newBuilder()
            .setValue("2")
            .build())
        .build()).join();
    manager.stop().join();
  }
}
