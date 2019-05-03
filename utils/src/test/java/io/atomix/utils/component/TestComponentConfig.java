package io.atomix.utils.component;

import io.atomix.utils.config.Config;

/**
 * Test component configuration.
 */
public class TestComponentConfig implements Config {
  private TestComponent1Config testComponent1Config = new TestComponent1Config();
  private TestComponent2Config testComponent2Config = new TestComponent2Config();

  public TestComponent1Config getTestComponent1Config() {
    return testComponent1Config;
  }
}
