package io.atomix.utils.component;

import io.atomix.utils.config.Config;

/**
 * Test component configuration.
 */
public class TestComponent1Config implements Config {
  private String value = "1";

  public String getValue() {
    return value;
  }
}
